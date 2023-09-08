;;; derl.el --- Erlang distribution protocol implementation  -*- lexical-binding: t -*-

;; Author: Axel Forsman <axel@axelf.se>
;; Keywords: comm, extensions, languages, processes
;; Version: 0.1.0
;; Package-Requires: ((emacs "29.1"))

;;; Commentary:

;; This library implements the Erlang distribution protocol as
;; described in
;; https://www.erlang.org/doc/apps/erts/erl_dist_protocol.html.

;; Start with "erl -sname arnie"

;;; Code:

(eval-when-compile (require 'cl-lib))
(require 'pcase)
(require 'generator)

(eval-when-compile
  (defmacro derl--read-uint (n)
    "Read N-byte network-endian unsigned integer."
    (cl-loop
     for i below n collect
     (let ((x `(get-byte ,(cl-case i
                            (0 'p)
                            (1 `(1+ p))
                            (t `(+ p ,i))))))
       (if (< i (1- n)) `(ash ,x ,(* 8 (- n i 1))) x))
     into xs finally return `(let ((p (point))) (forward-char ,n) (logior ,@xs))))

  (defmacro derl--uint-string (n integer)
    (macroexp-let2 nil x integer
      (cl-loop
       for i below n with xs do
       (push `(logand ,(if (= i 0) x `(ash ,x ,(* -8 i))) #xff) xs)
       finally return `(unibyte-string ,@xs)))))

;;; Erlang Port Mapper Daemon (EPMD) interaction

(defun derl-epmd-port-please (name &optional callback)
  "Get the distribution port of the node NAME.
If non-nil CALLBACK, return the network process and call CALLBACK
asynchronously with the port, nil indicating an error. Otherwise,
return the port in a blocking fashion."
  (if (null callback)
      (let* (result
             (proc (derl-epmd-port-please name (lambda (x) (setq result x)))))
        (while (accept-process-output proc))
        result)
    (let ((proc
           (make-network-process
            :name "epmd-port-please-req" :host 'local :service 4369
            :plist (list 'buffer "") :coding 'binary :filter
            (lambda (proc string)
              (setq string (concat (process-get proc 'buffer) string))
              (if (length< string 4)
                  (process-put proc 'buffer string)
                (set-process-sentinel proc nil)
                (delete-process proc)
                (cl-assert (eq (aref string 0) 119)) ; PORT2_RESP
                (let* ((result (aref string 1))
                       (portno (when (= result 0)
                                 (logior (ash (aref string 2) 8) (aref string 3)))))
                  (funcall callback portno))))
            :sentinel (lambda (proc event)
                        (unless (process-live-p proc) (funcall callback nil))))))
      (process-send-string
       proc (concat (let ((len (1+ (string-bytes name))))
                      (list (logand #xff (ash -8 len)) (logand #xff len)))
                    [122] ; PORT_PLEASE2_REQ
                    name))
      proc)))

;;; Erlang External Term Format

;; Unless messages are passed between connected nodes and a
;; distribution header is used the first byte should contain the
;; version number.
(defconst derl-ext-version 131 "Erlang external term format version magic.")

(defconst derl-tag (make-symbol "ext-tag")
  "Marks the encompassing vector as a special Erlang term (i.e. not a tuple).")

(defun derl-read ()
  "Read term encoded according to the Erlang external term format following point."
  (when (eq (char-after) 80) ; Compressed term
    (delete-char 5) ; Skip tag and uncompressed size
    (unless (zlib-decompress-region (point) (point-max))
      (error "Failed to decompress term")))
  (cl-labels
      ((read2 ()
         (forward-char 2)
         (logior (ash (char-before (1- (point))) 8) (char-before)))
       (read4 ()
         (let ((p (point)))
           (forward-char 4)
           (logior (ash (get-byte p) 24) (ash (get-byte (1+ p)) 16)
                   (ash (get-byte (+ p 2)) 8) (get-byte (+ p 3)))))
       (internal-p (node creation)
         (let ((conn (gethash node derl--connections)))
           (and (eq node (process-get conn 'name))
                (= creation (process-get conn 'creation))))))
    (forward-char)
    (pcase (char-before)
      (97 ; SMALL_INTEGER_EXT
       (forward-char 1)
       (char-before))
      (98 ; INTEGER_EXT
       (let ((u (read4)))
         (- (logand u #x7fffffff) (logand u #x80000000))))

      (88 ; NEW_PID_EXT
       (let ((node (derl-read)) (id (read4)) (serial (read4)) (creation (read4)))
         (when (internal-p node creation) (setq node nil creation nil))
         `[,derl-tag pid ,node ,id ,serial ,creation]))
      ((or (and 104 (let n (progn (forward-char) (char-before)))) ; SMALL_TUPLE_EXT
           (and 105 (let n (read4)))) ; LARGE_TUPLE_EXT
       (cl-loop repeat n collect (derl-read) into xs
                finally return (apply #'vector xs)))

      (116 ; MAP_EXT
       (cl-loop with n = (read4) with x = (make-hash-table :test 'equal :size n)
                repeat n do (puthash (derl-read) (derl-read) x) finally return x))
      (106) ; NIL_EXT
      (107 ; STRING_EXT
       (cl-loop repeat (read2) collect (progn (forward-char) (char-before))))
      (108 ; LIST_EXT
       (let ((xs (cl-loop repeat (read4) collect (derl-read))))
         (setcdr (last xs) (derl-read))
         xs))
      (109 ; BINARY_EXT
       (let ((n (read4)))
         (forward-char n)
         (buffer-substring-no-properties (- (point) n) (point))))
      ((or (and 110 (let n (progn (forward-char) (char-before)))) ; SMALL_BIG_EXT
           (and 111 (let n (read4)))) ; LARGE_BIG_EXT
       (cl-loop with sign = (progn (forward-char) (char-before)) and x = 0
                for i below n do
                (setq x (logior x (ash (get-byte) (ash i 1))))
                (forward-char)
                finally return (if (= sign 0) x (- x))))
      (90 ; NEWER_REFERENCE_EXT
       (let ((len (read2)) (node (derl-read)) (creation (read4)) (id 0))
         (dotimes (_ len) (setq id (logior (ash id 32) (read4))))
         (when (internal-p node creation) (setq node nil creation nil))
         `[,derl-tag reference ,node ,id ,creation]))

      (70 ; NEW_FLOAT_EXT
       (let* ((x (read4))
              (e (logand (ash x -20) #x7ff))
              (f (ldexp (logior (ash (logand x #xfffff) 32) (read4)) -52))
              (bias 1023))
         (* (if (= (logand x (ash 1 31)) 0) 1 -1)
            (cl-case e
              (#x7ff (if (= f 0) 1e+INF 0e+NaN))
              (0 (ldexp f (1- bias)))
              (t (ldexp (1+ f) (- e bias)))))))
      ((or (and 118 (let n (read2))) ; ATOM_UTF8_EXT
           (and 119 (let n (progn (forward-char) (char-before))))) ; SMALL_ATOM_UTF8_EXT
       (forward-char n)
       (intern (decode-coding-region (- (point) n) (point) 'utf-8 t)))
      (tag (error "Unknown tag `%s'" tag)))))

(defvar derl--write-connection)
(put 'derl--write-connection 'variable-documentation
     "The current connection during `derl-write'.")

(defun derl-write (term)
  "Print TERM at point according to the Erlang external term format."
  (cl-labels
      ((write2 (i) (insert (logand (ash i -8) #xff) (logand i #xff)))
       (write4 (i)
         (insert (logand (ash i -24) #xff) (logand (ash i -16) #xff)
                 (logand (ash i -8) #xff) (logand i #xff))))
    (pcase term
      ((and (pred integerp) i)
       (cond
        ((<= 0 i #xff) (insert 97 i)) ; SMALL_INTEGER_EXT
        ((<= (- #x80000000) i #x7fffffff)
         (insert 98) ; INTEGER_EXT
         (write4 i))
        (t (cl-loop
            with p = (point) and n = 0 initially (insert (if (>= i 0) 0 (setq i (- i)) 1))
            while (> i 0) do (insert (logand i #xff)) (setq i (ash i -8) n (1+ n))
            finally (save-excursion
                      (goto-char p)
                      (if (<= n #xff) (insert 110 n) ; SMALL_BIG_EXT
                        (insert 111) ; LARGE_BIG_EXT
                        (write4 n)))))))
      (`[,(pred (eq derl-tag)) pid ,node ,id ,serial ,creation]
       (unless node
         (setq node (process-get derl--write-connection 'name)
               creation (process-get derl--write-connection 'creation)))
       (insert 88) ; NEW_PID_EXT
       (derl-write node)
       (write4 id)
       (write4 serial)
       (write4 creation))
      (`[,(pred (eq derl-tag)) reference ,node ,id ,creation]
       (unless node
         (setq node (process-get derl--write-connection 'name)
               creation (process-get derl--write-connection 'creation)))
       (insert 90) ; NEWER_REFERENCE_EXT
       (cl-loop with xs while (or (> id 0) (null xs)) do
                (push (logand id #xffffffff) xs) (setq id (ash id -32)) finally
                (write2 (length xs)) (derl-write node) (write4 creation)
                (dolist (x xs) (write4 x))))
      ((pred vectorp)
       (if (<= (length term) #xff) (insert 104 (length term)) ; SMALL_TUPLE_EXT
         (insert 105) ; LARGE_TUPLE_EXT
         (write4 (length term)))
       (cl-loop for x across term do (derl-write x)))
      ((pred hash-table-p)
       (insert 116) ; MAP_EXT
       (write4 (hash-table-count term))
       (maphash (lambda (k v) (derl-write k) (derl-write v)) term))
      ('nil (insert 106)) ; NIL_EXT
      ((pred consp) ; TODO Use STRING_EXT if possible
       (insert 108) ; LIST_EXT
       (let ((sp (point)) (n 1))
         (while (progn (derl-write (pop term)) (consp term)) (setq n (1+ n)))
         (derl-write term)
         (save-excursion (goto-char sp) (write4 n))))
      ((pred stringp)
       (insert 109) ; BINARY_EXT
       (let ((n (encode-coding-string term 'utf-8 nil (current-buffer))))
         (write4 n)
         (forward-char n)))
      ((pred floatp)
       (let* ((exp (frexp term)) (sgnfcand (pop exp))
              (sign (if (>= sgnfcand 0) 0 (setq sgnfcand (- sgnfcand)) (ash 1 31)))
              (bias 1023) e f)
       (insert 70) ; NEW_FLOAT_EXT
       (cond
        ((isnan term) (setq e #x7ff f 1))
        ((eq sgnfcand 1e+INF) (setq e #x7ff f 0))
        ((<= exp (- 1 bias)) ; Subnormals
         (setq e 0 f (floor (ldexp sgnfcand (+ 52 exp (1- bias))))))
        ;; Ensure significand >= 1 by decrementing exponent
        (t (setq e (+ bias exp -1)
                 f (floor (ldexp (1- (* 2 sgnfcand)) 52)))))
       (write4 (logior sign (ash e 20) (logand (ash f -32) #xfffff)))
       (write4 (logand f #xffffffff))))
      ((pred symbolp)
       (let ((n (encode-coding-string
                 (symbol-name term) 'utf-8 nil (current-buffer))))
         (if (<= n #xff) (insert 119 n) ; SMALL_ATOM_UTF8_EXT
           (insert 118) ; ATOM_UTF8_EXT
           (write4 n))
         (forward-char n)))
      (_ (signal 'wrong-type-argument (list term))))))

;;; Erlang-like processes

(cl-defstruct (derl--process (:type vector) (:constructor nil)
                             (:copier nil) (:predicate nil))
  (id (:read-only t)) (function (:read-only t)) mailbox blocked exits)

;; Process-local variables
(defvar derl--self [0 nil () nil ()] "The current `derl--process'.")
(defvar derl--mailbox ()
  "Local chronological order mailbox of the current process.")

(defvar derl--processes
  (let ((table (make-hash-table)))
    (puthash (derl--process-id derl--self) derl--self table)
    table)
  "Map of PID:s to processes.")
(defvar derl--registry (make-hash-table :test 'eq) "Process registry.")
(defvar derl--next-pid 0)
(defvar derl--next-ref 0)

(define-error 'normal "Normal exit" 'normal)

(defun derl-self ()
  "Return the process identifier of the calling process."
  `[,derl-tag pid nil ,(derl--process-id derl--self) 0 nil])

(defun derl-make-ref ()
  "Return a unique reference."
  (let ((id derl--next-ref))
    (when (> (ash (cl-incf derl--next-ref) (* -5 32)) 0) (setq derl--next-ref 0))
    `[,derl-tag reference nil ,id nil]))

(defun derl-register (name pid)
  "Register a symbol NAME with PID in the name registry."
  (pcase-exhaustive pid
    ((and `[,_ pid nil ,id ,_ nil] (guard (gethash id derl--processes)))
     (puthash name id derl--registry))
    ('nil (remhash name derl--registry))))

(defun derl-whereis (name)
  "Return the process identifier with the registered NAME, or nil if none exists."
  (let ((id (gethash name derl--registry)))
    (and id (gethash id derl--processes) `[,derl-tag pid nil ,id 0 nil])))

(defvar derl--waiting nil)
(defvar derl--scheduler-timer nil)
(defun derl--schedule (&optional force)
  (if (and force derl--waiting) (throw 'derl--wake nil)
    (unless derl--scheduler-timer
      (setq derl--scheduler-timer (run-with-idle-timer 0 nil #'derl--run)))))

(defun derl--run ()
  (setq derl--scheduler-timer nil)
  (when (/= (derl--process-id derl--self) 0) (error "Scheduling from inferior process"))
  (while (let* ((schedulable
                 (cl-loop for p being the hash-values of derl--processes
                          unless (derl--process-blocked p) collect p))
                (proc (and schedulable (nth (random (length schedulable)) schedulable))))
           (cond
            (derl--waiting nil)
            ((null proc) ; Blocked on externalities
             (let ((derl--waiting t))
               (catch 'derl--wake (accept-process-output nil 30)) t))
            ((eq proc derl--self) ; Pass control back to main process
             (when (cdr schedulable) (derl--schedule) nil))
            (t (let ((id (derl--process-id proc)) (derl--self proc))
                 (condition-case err (iter-next (derl--process-function proc))
                   ((iter-end-of-sequence normal) (remhash id derl--processes))
                   (t (remhash id derl--processes)
                      (message "Process %d exited with error: %S" id err))))
               t)))))

(defun derl-spawn (fun)
  "Return the process identifier of a new process started by delegating to FUN.
FUN should be a generator."
  (let* ((id (cl-incf derl--next-pid)) m
         (f (lambda (op value)
              (let ((derl--mailbox m))
                (unwind-protect (funcall fun op value) (setq m derl--mailbox))))))
    (puthash id (vector id f () nil ()) derl--processes)
    (derl--schedule)
    `[,derl-tag pid nil ,id 0 nil]))

(cl-defmacro derl-yield (&environment env)
  "Try to give other processes a chance to execute before returning."
  `(progn
     (unless (derl--process-exits derl--self)
       ,(if (assq 'iter-yield env) '(iter-yield nil) '(derl--run)))
     (when-let (x (pop (derl--process-exits derl--self))) (signal x nil))))

(cl-defmacro derl-receive
    (&rest arms &aux (cell (make-symbol "cell")) (prev (make-symbol "prev"))
           (result (make-symbol "result")) (continue (make-symbol "continue"))
           (timer (make-symbol "timer")) timeout on-timeout)
  "Wait for message matching one of ARMS and proceed with its action.

\(fn ARMS... [:after SECS TIMEOUT-FORM])"
  (declare (debug ([&rest (pcase-PAT body)] &optional [":after" sexp form])))
  (pcase (last arms 3)
    (`(:after ,secs ,form) (setq arms (nbutlast arms 3) timeout secs on-timeout form)))
  `(cl-loop
    ,@(when timeout
        `(with ,timer initially
               (let ((f (lambda (p) (setf (derl--process-blocked p) nil ,timer nil)
                          (derl--schedule t))))
                 (setq ,timer (run-with-timer ,timeout nil f derl--self)))))
    for ,cell =
    (or (if ,cell (cdr ,cell) derl--mailbox)
        (while (null (derl--process-mailbox derl--self))
          (cl-letf (((derl--process-blocked derl--self) t))
            ,@(when timeout `((unless ,timer (cl-return ,on-timeout))))
            (derl-yield)))
        (let ((xs (nreverse (derl--process-mailbox derl--self))))
          (setf (derl--process-mailbox derl--self) ()
                (if ,cell (cdr ,cell) derl--mailbox) xs)))
    and ,prev = ,cell with ,result while
    (let ((,continue nil))
      (setq ,result ,(let* ((x (gensym "_"))
                            (pcase--dontwarn-upats (cons x pcase--dontwarn-upats)))
                       (pcase--expand `(car ,cell) `(,@arms (,x (setq ,continue t))))))
      ,continue)
    finally (if ,prev (setcdr ,prev (cdr ,cell)) (pop derl--mailbox))
    ,@(when `((when ,timer (cancel-timer ,timer)))) finally return ,result))

(defun derl-send (dest msg)
  "Send MSG to DEST and return MSG.
DEST can be a remote or local process identifier, a locally registered
name, or a tuple \(REG-NAME . NODE) for a name at another node."
  (when-let
      ((id (pcase-exhaustive dest
             (`[,_ pid nil ,id ,_ nil] id)
             ((pred symbolp) (gethash dest derl--registry))
             ((or (and `(,name . ,node)
                       (let ctl `[6 ,(derl-self) nil ,name])) ; REG_SEND
                  (and `[,_ pid ,node ,_ ,_ ,_]
                       (let ctl `[22 ,(derl-self) ,dest]))) ; SEND_SENDER
              (when-let (conn (gethash node derl--connections))
                (if (and name (eq node (process-get conn 'name)))
                    (gethash name derl--registry)
                  (derl--send-control-msg conn ctl msg)
                  nil)))))
       (process (gethash id derl--processes)))
    (push msg (derl--process-mailbox process))
    (setf (derl--process-blocked process) nil)
    (derl--schedule))
  msg)

(defun derl-exit (pid reason)
  "Send an exit signal with exit REASON to the process identified by PID."
  (pcase-let ((`[,_ pid ,node ,id ,_ ,_] pid))
    (if node
        (when-let (conn (gethash node derl--connections))
          (derl--send-control-msg conn `[8 ,(derl-self) ,pid ,reason])) ; EXIT2
      (when-let (process (gethash id derl--processes))
        (push reason (derl--process-exits process))
        (setf (derl--process-blocked process) nil)
        (derl--schedule)))))

;;; Erlang Distribution Protocol

(defvar derl--connections (make-hash-table :test 'eq :weakness 'value)
  "Map from Erlang node names to connections.")

(defun derl--gen-digest (challenge cookie)
  "Generate a message digest (the \"gen_digest()\" function)."
  (secure-hash 'md5 (concat cookie (number-to-string challenge)) nil nil t))

(defun derl-connect (host port cookie)
  (cl-labels
      ((recv-status (proc)
         (unless (eq (get-byte) ?s) (error "Bad status"))
         (forward-char)
         (cond
          ((looking-at-p "named:")
           (forward-char 6)
           (let* ((nlen (derl--read-uint 2))
                  (name (progn (forward-char nlen)
                               (buffer-substring-no-properties (- (point) nlen) (point))))
                  (creation (derl--read-uint 4)))
             (message "Name: %s, creation: %d" name creation)
             (process-put proc 'name (intern name))
             (process-put proc 'creation creation)
             (process-put proc 'filter #'recv-challenge)))
          ;; TODO alive and case 3B)
          (t (error "Unknown status"))))
       (recv-challenge (proc)
         (cl-assert (eq (get-byte) ?N)) ; recv_challenge_reply tag
         (forward-char 9)
         (let* ((challenge-b (derl--read-uint 4))
                (creation-b (derl--read-uint 4))
                (nlen (derl--read-uint 2))
                (name-b (progn (forward-char nlen)
                               (buffer-substring-no-properties (- (point) nlen) (point))))
                (challenge-a (random (ash 1 32))) ; #x100000000
                (digest (derl--gen-digest challenge-b cookie)))
           (process-put proc 'name-b (intern name-b))
           (process-put proc 'creation-b creation-b)
           (process-put proc 'challenge-a challenge-a)
           (process-send-string
            proc (concat [0 21 ; Length
                            ?r] ; send_challenge_reply tag
                         (derl--uint-string 4 challenge-a)
                         digest))
           (process-put proc 'filter #'recv-challenge-ack)))
       (recv-challenge-ack (proc)
         (unless (eq (get-byte) ?a) (error "Bad tag"))
         (let ((challenge-a (process-get proc 'challenge-a))
               (digest (buffer-substring-no-properties (1+ (point)) (+ (point) 1 16))))
           (forward-char 17)
           (unless (string= (derl--gen-digest challenge-a cookie) digest)
             (error "Bad digest"))
           (message "Got challenge ACK!")
           (puthash (process-get proc 'name) proc derl--connections)
           (puthash (process-get proc 'name-b) proc derl--connections)
           (process-put proc 'filter #'connected)))
       (connected (proc)
         (if (= (point-min) (point-max))
             (process-send-string proc "\0\0\0\0") ; Zero-length heartbeat
           (cl-assert (eq (get-byte) 112)) ; Check that type is pass through
           (forward-char)
           (let ((ctl (progn (cl-assert (eq (get-byte) derl-ext-version))
                             (forward-char)
                             (derl-read)))
                 (msg (unless (eobp)
                        (cl-assert (eq (get-byte) derl-ext-version))
                        (forward-char)
                        (derl-read))))
             (message "Parsed: %S" (cons ctl msg))
             (pcase ctl
               (`[22 ,_from ,to] (! to msg)) ; SEND_SENDER
               (`[,(or 3 8) ,from ,to ,reason] (derl-exit to reason)) ; EXIT/EXIT2
               (`[6 ,_from ,_ ,to] (! to msg)))))) ; REG_SEND
       (filter (proc string)
         (with-current-buffer (process-buffer proc)
           (insert string)
           (goto-char (point-min))
           (let (lenlen len)
             (while (and (<= (setq lenlen (if (eq (process-get proc 'filter) #'connected) 4 2))
                             (buffer-size))
                         (<= (setq len (+ lenlen (if (= lenlen 4) (derl--read-uint 4)
                                                   (derl--read-uint 2))))
                             (buffer-size)))
               (save-restriction
                 (narrow-to-region (point) (+ (point-min) len))
                 (funcall (process-get proc 'filter) proc)
                 (when (< (point) (point-max)) (error "Bad length")))
               (delete-region (point-min) (point))))
           (goto-char (point-max))))
       (sentinel (proc _event)
         (unless (process-live-p proc) (kill-buffer (process-buffer proc)))))
    (let* ((buf (generate-new-buffer " *erl recv*" t))
           (proc (make-network-process
                  :name "erl-emacs" :buffer buf :host host :service port
                  :coding 'binary :filter #'filter :sentinel #'sentinel
                  :plist (list 'filter #'recv-status)))
           (flags
            (eval-when-compile
              (derl--uint-string
               8 (logior
                  #x4 ; DFLAG_EXTENDED_REFERENCES
                  #x10 ; DFLAG_FUN_TAGS
                  #x80 ; DFLAG_NEW_FUN_TAGS
                  #x100 ; DFLAG_EXTENDED_PID_PORTS
                  #x200 ; DFLAG_EXPORT_PTR_TAG
                  #x400 ; DFLAG_BIT_BINARIES
                  #x800 ; DFLAG_NEW_FLOATS
                  #x10000 ; DFLAG_UTF8_ATOMS
                  #x20000 ; DFLAG_MAP_TAG
                  #x40000 ; DFLAG_BIG_CREATION
                  #x80000 ; DFLAG_SEND_SENDER
                  #x1000000 ; DFLAG_HANDSHAKE_23
                  #x2000000 ; DFLAG_UNLINK_ID
                  (ash 1 36) ; DFLAG_MANDATORY_25_DIGEST
                  (ash 1 33) ; DFLAG_NAME_ME
                  (ash 1 34))))) ; DFLAG_V4_NC
           (name (system-name)))
      (with-current-buffer buf (set-buffer-multibyte nil))
      ;; Send send_name message
      (process-send-string
       proc (concat (derl--uint-string 2 (+ 15 (string-bytes name))) ; Length
                    [?N] ; send_name tag
                    flags
                    [0 0 0 0] ; Creation
                    (derl--uint-string 2 (string-bytes name)) ; Nlen
                    name))
      proc)))

(defun derl--send-control-msg (conn control-msg &optional msg)
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (insert 112 derl-ext-version) ; Pass through
    (let ((derl--write-connection conn))
      (derl-write control-msg)
      (when msg (insert derl-ext-version) (derl-write msg)))
    (goto-char (point-min))
    (insert (derl--uint-string 4 (buffer-size)))
    (process-send-region conn (point-min) (point-max))))

(cl-defun derl-cookie (&aux file)
  (when (or (file-exists-p (setq file "~/.erlang.cookie"))
            (file-exists-p
             (setq file (concat (or (getenv "XDG_CONFIG_HOME") "~/.config")
                                "/erlang/.erlang.cookie"))))
    (with-temp-buffer (insert-file-contents-literally file)
                      (buffer-string))))

(iter-defun derl-rpc (node module function args)
  "Apply FUNCTION in MODULE to ARGS on the remote NODE."
  (! `(rex . ,node)
     ;; {Who, {call, M, F, A, GroupLeader}}
     (let ((pid (derl-self))) `[,pid [call ,module ,function ,args ,pid]]))
  (derl-receive (`[rex ,x] x)))

(defun derl-do (gen)
  "Like `iter-yield-from' for contexts callable only from the main process."
  (iter-do (_ gen) (derl--run)))

(cl-defun derl-call (fun &key (timeout 5))
  "Delegate to generator FUN with TIMEOUT, dropping any laggard messages."
  (iter-make
   (let* ((self (derl-self))
          (ref (derl-make-ref))
          (pid (derl-spawn
                (lambda (op value)
                  (condition-case err (funcall fun op value)
                    (iter-end-of-sequence (! self (cons ref (cdr err)))
                                          (signal (car err) (cdr err))))))))
     (condition-case err
         (derl-receive (`(,(pred (equal ref)) . ,x) x)
                       :after timeout (progn (derl-exit pid 'normal) 'timeout))
       (t (derl-exit pid 'normal)
          (setf derl--mailbox (assoc-delete-all ref derl--mailbox)
                (derl--process-mailbox derl--self)
                (assoc-delete-all ref (derl--process-mailbox derl--self)))
          (signal (car err) (cdr err)))))))

(provide 'derl)

;; Local Variables:
;; read-symbol-shorthands: (("!" . "derl-send"))
;; End:

;;; derl.el ends here
