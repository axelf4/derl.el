;;; derl.el --- Erlang distribution protocol implementation  -*- lexical-binding: t -*-

;;; Commentary:

;; Start with "erl -sname arnie" and get cookie

(eval-when-compile (require 'cl-lib))
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

(defun derl-epmd-port-please (name &optional cb)
  "Get the distribution port of the node NAME."
  (if (null cb)
      (let* (result
             (proc (derl-epmd-port-please name (lambda (x) (setq result x)))))
        (while (accept-process-output proc))
        result)
    (let ((proc
           (make-network-process
            :name "epmd-port-req" :host 'local :service 4369 :plist (list 'buffer "")
            :coding 'raw-text :filter-multibyte nil :filter
            (lambda (proc string)
              (setq string (concat (process-get proc 'buffer) string))
              (if (length< string 4)
                  (process-put proc 'buffer string)
                (set-process-sentinel proc nil)
                (delete-process proc)
                (cl-assert (eq (aref string 0) 119)) ; PORT2_RESP
                (let ((result (aref string 1)))
                  (funcall
                   cb (if (/= result 0) (cons 'error result)
                        (logior (ash (aref string 2) 8) (aref string 3))))))) ; portno
            :sentinel (lambda (proc event) (funcall cb (cons 'error event))))))
      (process-send-string
       proc (concat (derl--uint-string 2 (1+ (string-bytes name)))
                    [122] ; PORT_PLEASE2_REQ
                    name))
      proc)))

;;; Erlang External Term Format

;; Unless messages are passed between connected nodes and a
;; distribution header is used the first byte should contain the
;; version number.
(defconst derl-ext-version 131)

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
                   (ash (get-byte (+ p 2)) 8) (get-byte (+ p 3))))))
    (forward-char)
    (pcase (char-before)
      (97 ; SMALL_INTEGER_EXT
       (forward-char 1)
       (char-before))
      (98 ; INTEGER_EXT
       (let ((u (read4)))
         (- (logand u #x7fffffff) (logand u #x80000000))))

      (88 ; NEW_PID_EXT
       (let* ((node (derl-read))
              (id (read4))
              (serial (read4))
              (creation (read4)))
         (vector derl-tag 'pid node id serial creation)))
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
       (let* ((len (read2))
              (node (derl-read))
              (creation (read4))
              (id (cl-loop repeat len collect (read4))))
         (vector derl-tag 'reference node id creation)))

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

(defun derl-write (term)
  "Print TERM at point according to the Erlang external term format."
  (cl-labels
      ((write4 (i)
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
       (insert 88) ; NEW_PID_EXT
       (derl-write node)
       (write4 id)
       (write4 serial)
       (write4 creation))
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
      (_ (error "Cannot encode the term `%S'" term)))))

(defun derl-binary-to-term (&rest args)
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (apply #'insert args)
    (goto-char (point-min))
    (unless (eq (char-after) derl-ext-version) (error "Bad version"))
    (forward-char)
    (derl-read)))

(defun derl-term-to-binary (term)
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (insert derl-ext-version)
    (derl-write term)
    (buffer-substring-no-properties (point-min) (point-max))))

;;; Erlang-like processes

(cl-defstruct (derl-process (:type vector) (:constructor nil)
                            (:copier nil) (:predicate nil))
  (id (:read-only t)) function mailbox blocked exits)

;; Process-local variables
(defvar derl--self [0 nil () nil ()]
  "The current `derl-process'.")
(defvar derl--mailbox ()
  "The local mailbox of the current process.")

(defvar derl--processes
  (let ((table (make-hash-table :test 'eql)))
    (puthash (derl-process-id derl--self) derl--self table)
    table)
  "Map of PID:s to processes.")
(defvar derl--next-pid 0)
(defvar derl--next-ref 0)

(define-error 'normal "Normal exit" 'normal)

(defun derl-make-ref ()
  (prog1 derl--next-ref (setq derl--next-ref (1+ derl--next-ref))))

(defvar derl--scheduler-timer nil)

(defun derl--scheduler-schedule ()
  (unless derl--scheduler-timer
    (setq derl--scheduler-timer (run-with-timer 0 nil #'derl--scheduler-run))))

(defun derl--scheduler-run ()
  (unless (= (derl-process-id derl--self) 0) (error "Scheduling from inferior process"))
  (setq derl--scheduler-timer nil)
  (while (let* ((schedulable
                 (cl-loop for p being the hash-values of derl--processes
                          unless (derl-process-blocked p) collect p))
                (proc (and schedulable (nth (random (length schedulable)) schedulable))))
           (cond
            ;; Main process is blocked on externalities
            ((null schedulable) (accept-process-output nil 1))
            ;; Pass control back to main process
            ((= (derl-process-id proc) 0)
             (when (cdr schedulable) (derl--scheduler-schedule))
             nil)
            (t (let ((id (derl-process-id proc)) (derl--self proc))
                 (condition-case err (iter-next (derl-process-function proc))
                   ((iter-end-of-sequence normal) (remhash id derl--processes))
                   (t (remhash id derl--processes)
                      (message "Process %d exited with error: %S" id err))))
               t)))))

(defun derl-spawn (fun)
  (let* ((id (cl-incf derl--next-pid))
         (process (vector id nil () nil ())))
    (setf (derl-process-function process)
          (iter-make (let ((derl--mailbox ())) (iter-yield-from fun))))
    (puthash id process derl--processes)
    (derl--scheduler-schedule)
    id))

(cl-defmacro derl-yield (&environment env)
  `(progn
     (unless (derl-process-exits derl--self)
       ,(if (assq 'iter-yield env) '(iter-yield nil) '(derl--scheduler-run)))
     (dolist (x (prog1 (derl-process-exits derl--self)
                  (setf (derl-process-exits derl--self) ())))
       (apply (car x) (cdr x)))))

(cl-defmacro derl-receive
    (&rest arms &aux (catchallp (cl-loop for (x) in arms thereis (symbolp x))))
  (declare (debug (&rest (pcase-PAT body))))
  `(cl-loop
    for cell =
    (or (if cell (cdr cell) derl--mailbox)
        (cl-letf (((derl-process-blocked derl--self) t))
          (while (null (derl-process-mailbox derl--self)) (derl-yield))
          (let ((xs (nreverse (derl-process-mailbox derl--self))))
            (setf (derl-process-mailbox derl--self) ())
            (if cell (setcdr cell xs) (setq derl--mailbox xs)))))
    and prev = cell with result while
    (let ((continue nil))
      (setq result (pcase (car cell) ,@arms ,@(unless catchallp '((_ (setq continue t))))))
      continue)
    finally (if prev (setcdr prev (cdr cell)) (pop derl--mailbox))
    finally return result))

(defun derl-send (pid expr &optional conn)
  "Send the message EXPR to PID, optionally over CONN."
  (message "Sending %S to %S" expr pid)
  (if (null conn)
      (when-let (process (gethash pid derl--processes))
        (push expr (derl-process-mailbox process))
        (derl--scheduler-schedule)
        (setf (derl-process-blocked process) nil))
    (let* ((from (derl-self conn))
           (control-msg
            (derl-term-to-binary
             (if (symbolp pid) `[6 ,from nil ,pid] ; REG_SEND
               (let* ((name (process-get conn 'name-b))
                      (creation (process-get conn 'creation-b))
                      (to `[,derl-tag pid ,name ,pid 0 ,creation]))
                 `[22 ,from ,to])))) ; SEND_SENDER
           (msg (derl-term-to-binary expr)))
      (process-send-string
       conn
       (concat
        (derl--uint-string 4 (+ 1 (string-bytes control-msg) (string-bytes msg)))
        [112] ; Pass through
        control-msg msg)))))

(defun derl-exit (pid &rest args)
  "Signal the exit REASON to PID."
  (message "Got thing for %S , %S" pid args)
  (when-let (process (gethash pid derl--processes))
    (push args (derl-process-exits process))
    (derl--scheduler-schedule)))

;;; Erlang Distribution Protocol

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
             (message "Name: %s, creation: %s" name creation)
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
           (process-send-string
            proc (concat [0 21 ; Length
                            ?r] ; send_challenge_reply tag
                         (derl--uint-string 4 challenge-a)
                         digest))
           (process-put proc 'filter #'recv-challenge-ack)))
       (recv-challenge-ack (proc)
         (unless (eq (get-byte) ?a) (error "Bad tag"))
         (let ((digest (buffer-substring-no-properties (1+ (point)) (+ (point) 1 16))))
           (forward-char 17)
           (message "Got challenge ACK: digest %S!" digest)
           (process-put proc 'filter #'connected)))
       (connected (proc)
         (if (= (point-min) (point-max))
             (process-send-string proc "\0\0\0\0") ; Zero-length heartbeat
           (cl-assert (eq (get-byte) 112)) ; Check that type is pass through
           (forward-char)
           (let ((control-msg
                  (progn (cl-assert (eq (get-byte) derl-ext-version))
                         (forward-char)
                         (derl-read)))
                 (msg (progn (cl-assert (eq (get-byte) derl-ext-version))
                             (forward-char)
                             (derl-read))))
             (message "Parsed: %S" (cons control-msg msg))
             (pcase control-msg
               ;; SEND_SENDER
               (`[22 ,_from-pid [,_ pid ,_name ,to-pid ,_serial ,_creation]]
                (! to-pid msg))))))
       (filter (proc string)
         (with-current-buffer (process-buffer proc)
           (insert string)
           (goto-char (point-min))
           (let ((lenlen (if (eq (process-get proc 'filter) #'connected) 4 2))
                 len)
             (while (and (<= lenlen (buffer-size))
                         (<= (setq len (+ lenlen (if (= lenlen 4) (derl--read-uint 4)
                                                   (derl--read-uint 2))))
                             (buffer-size)))
               (let ((f (process-get proc 'filter)))
                 (save-restriction
                   (narrow-to-region (point) (+ (point-min) len))
                   (funcall f proc)
                   (when (< (point) (point-max)) (error "Bad length")))
                 (delete-region (point-min) (+ (point-min) len)))))
           (goto-char (point-max)))))
    (let* ((buf (generate-new-buffer " *erl recv*" t))
           (proc (make-network-process
                  :name "erl-emacs" :buffer buf :host host :service port
                  :coding 'raw-text :filter-multibyte nil :filter #'filter
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
                  (ash 1 33) ; DFLAG_NAME_ME
                  (ash 1 34))))) ; DFLAG_V4_NC
           (name (system-name)))
      (with-current-buffer buf
        (set-buffer-multibyte nil)
        (buffer-disable-undo))
      ;; Send send_name message
      (process-send-string
       proc (concat (derl--uint-string 2 (+ 15 (string-bytes name))) ; Length
                    [?N] ; send_name tag
                    flags
                    [0 0 0 0] ; Creation
                    (derl--uint-string 2 (string-bytes name)) ; Nlen
                    name))
      proc)))

(defun derl-self (conn)
  "Return an Erlang PID object for this process on the node connection CONN."
  (let ((name (process-get conn 'name))
        (id (derl-process-id derl--self))
        (serial 0)
        (creation (process-get conn 'creation)))
    `[,derl-tag pid ,name ,id ,serial ,creation]))

(defun derl-rpc (conn module function &rest args)
  "Apply FUNCTION in MODULE to ARGS on the remote node over CONN."
  (let ((pid (derl-self conn)))
    ;; {Who, {call, M, F, A, GroupLeader}}
    (! 'rex `[,pid [call ,module ,function ,args ,pid]] conn))
  (derl-receive (`[rex ,x] x)))

;; For simplicity this function may only be called from the main process
(defun derl--call (fun)
  "Delegate to generator FUN ensuring that spam is blocked."
  (let* ((self (derl-process-id derl--self))
         (ref (derl-make-ref))
         (pid (derl-spawn (iter-make (! self (cons ref (iter-yield-from fun)))))))
    (with-timeout (1 (derl-exit pid #'signal 'normal nil) 'timeout)
      (derl-receive (`(,(pred (equal ref)) . ,x) x)))))

(provide 'derl)

;; Local Variables:
;; read-symbol-shorthands: (("!" . "derl-send"))
;; End:

;;; derl.el ends here