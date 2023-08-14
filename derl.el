;;; derl.el --- Erlang distribution protocol implementation -*- lexical-binding: t -*-

;; Start with "erl -sname arnie" and get cookie

(eval-when-compile (require 'cl-lib))

(defmacro derl--read-uint (n string offset)
  "Read N-byte network-endian unsigned integer from STRING at OFFSET."
  (macroexp-let2* symbolp ((s string) (j offset))
    (cl-loop
     for i below n collect
     (let ((x `(aref ,string ,(cl-case i
                                (0 j)
                                (1 `(1+ ,j))
                                (t `(+ ,j ,i))))))
       (if (< i (1- n)) `(ash ,x ,(* 8 (- n i 1))) x))
     into xs finally return `(logior ,@xs))))

(defmacro derl--write-uint (n integer)
  (macroexp-let2 symbolp x integer
    (cl-loop
     for i below n with xs do
     (push `(logand ,(if (= i 0) x `(ash ,x ,(* -8 i))) #xff) xs)
     finally return `(unibyte-string ,@xs))))

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
            :name "epmd-port-req" :host 'local :service 4369
            :coding 'raw-text :filter-multibyte nil :filter
            (lambda (proc string)
              (setq string (concat (process-get proc 'buf) string))
              (if (length< string 4)
                  (process-put proc 'buf string)
                (set-process-sentinel proc nil)
                (delete-process proc)
                (cl-assert (eq (aref string 0) 119)) ; PORT2_RESP
                (let ((result (aref string 1)))
                  (funcall
                   cb (if (/= result 0) (cons 'error result)
                        (derl--read-uint 2 string 2)))))) ; portno
            :sentinel (lambda (proc event) (funcall cb (cons 'error event)))
            :plist (list 'buf ""))))
      (process-send-string
       proc (concat (derl--write-uint 2 (1+ (string-bytes name)))
                    [122] ; PORT_PLEASE2_REQ
                    name))
      proc)))

;;; Erlang External Term Format

;; Unless messages are passed between connected nodes and a
;; distribution header is used the first byte should contain the
;; version number.
(defconst derl-ext-version 131)

(defconst derl-tag (make-symbol "ext-tag")
  "Marks the encompassing vector as a special Erlang object (i.e. not a tuple).")

(defun derl-read ()
  "Read the Erlang external term following point."
  (when (eq (char-after) 80) ; Compressed term
    (delete-char 5) ; Skip tag and uncompressed size
    (unless (zlib-decompress-region (point) (point-max))
      (error "Failed to decompress term")))
  (cl-labels
      ((read2 ()
         (forward-char 2)
         (logior (ash (char-before (1- (point))) 8) (char-before)))
       (read4 ()
         (forward-char 4)
         (let ((pos (point)))
           (logior (ash (char-before (- pos 3)) 24) (ash (char-before (- pos 2)) 16)
                   (ash (char-before (1- pos)) 8) (char-before)))))
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

      (106) ; NIL_EXT
      (107 ; STRING_EXT
       (cl-loop repeat (read2) collect (progn (forward-char) (char-before))))
      (108 ; LIST_EXT
       (cl-loop repeat (read4) collect (derl-read) into xs
                finally (setcdr (last xs) (derl-read))))
      (109 ; BINARY_EXT
       (let ((n (read4)))
         (forward-char n)
         (buffer-substring-no-properties (- (point) n) (point))))
      ((or (and 110 (let n (progn (forward-char) (char-before)))) ; SMALL_BIG_EXT
           (and 111 (let n (read4)))) ; LARGE_BIG_EXT
       (cl-loop with sign = (progn (forward-char) (char-before))
                with x = 0 for i below n do
                (setq x (logior x (ash (char-after) (ash i 1))))
                (forward-char)
                finally return (if (= sign 0) x (- x))))

      ((or (and 118 (let n (read2)) (let enc 'utf-8)) ; ATOM_UTF8_EXT
           (and 119 (let n (progn (forward-char) (char-before)))
                (let enc 'utf-8)) ; SMALL_ATOM_UTF8_EXT
           (and 100 (let n (read2)) (let enc 'latin-1)) ; ATOM_EXT
           (and 115 (let n (progn (forward-char) (char-before)))
                (let enc 'latin-1))) ; SMALL_ATOM_EXT
       (forward-char n)
       (intern (decode-coding-region (- (point) n) (point) enc t)))
      (tag (error "Unknown tag `%s'" tag)))))

(defun derl-write (term)
  "Print the Erlang external representation of TERM at point."
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
        (t (error "TODO Write bignum"))))
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
      ('nil (insert 106)) ; NIL_EXT
      ((pred consp)
       (insert 108) ; LIST_EXT
       (let ((sp (point)) n)
         (while (progn (derl-write (pop term))
                       (setq n (1+ n))
                       (consp term)))
         (derl-write term)
         (save-excursion (goto-char sp) (write4 n))))
      ((pred symbolp)
       (let ((n (encode-coding-string
                 (symbol-name term) 'utf-8 t (current-buffer))))
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
  (id (:read-only t)) (cond-var (:read-only t)) mailbox thread)

;; Process-local variables
(defvar derl--self (vector 0 (make-condition-variable (make-mutex)) () main-thread)
  "The current `derl-process'.")
(defvar derl--mailbox ()
  "The local mailbox of the current process.")

(defvar derl--processes
  (let ((table (make-hash-table :test 'eql)))
    (puthash (derl-process-id derl--self) derl--self table)
    table)
  "Map of PID:s to processes.")
(defvar derl--next-pid 0)

(defun derl-spawn (fun)
  (let* ((id (cl-incf derl--next-pid))
         (mutex (make-mutex))
         (cond-var (make-condition-variable mutex))
         (process (vector id cond-var () nil))
         (thread
          (make-thread
           (lambda ()
             (let ((derl--self process) derl--mailbox)
               (unwind-protect (funcall fun)
                 (remhash (derl-process-id derl--self) derl--processes)))))))
    (setf (derl-process-thread process) thread)
    (puthash id process derl--processes)
    id))

(defmacro derl-receive (&rest arms)
  `(cl-loop
    with fetch =
    (lambda ()
      (with-mutex (condition-mutex (derl-process-cond-var derl--self))
        (while (null (derl-process-mailbox derl--self))
          (condition-wait (derl-process-cond-var derl--self)))
        (prog1 (nreverse (derl-process-mailbox derl--self))
          (setf (derl-process-mailbox derl--self) ()))))
    for cell = (or derl--mailbox (setq derl--mailbox (funcall fetch))) then
    (or (cdr cell)
        (let ((xs (funcall fetch)))
          (if cell (setcdr cell xs) (setq derl--mailbox xs))))
    and prev = cell with result while
    (let (continue)
      (setq result (pcase (car cell) ,@arms (_ (setq continue t))))
      continue)
    finally (if prev (setcdr prev (cdr cell)) (pop derl--mailbox))
    finally return result))

(defun derl-send (pid expr)
  (when-let (process (gethash pid derl--processes))
    (with-mutex (condition-mutex (derl-process-cond-var process))
      (push expr (derl-process-mailbox process))
      (condition-notify (derl-process-cond-var process)))))

;;; Erlang Distribution Protocol

(defun derl--gen-digest (challenge cookie)
  "Generate a message digest (the \"gen_digest()\" function)."
  (secure-hash 'md5 (concat cookie (number-to-string challenge)) nil nil t))

(cl-defun derl-connect (host port cookie)
  (cl-labels
      ((get-msg (proc string lenlen)
         (let* ((saved-s (process-get proc 'buf))
                (s (cond ((string= saved-s "") string)
                         ((string= string "") saved-s)
                         (t (concat saved-s string))))
                (s-len (string-bytes s)) length)
           (if (or (< s-len lenlen)
                   (> (setq len (+ lenlen (if (= lenlen 4) (derl--read-uint 4 s 0)
                                            (derl--read-uint 2 s 0))))
                      s-len))
               (progn (process-put proc 'buf s) nil)
             (process-put proc 'buf (substring s len))
             (substring s lenlen len))))
       (recv-status (proc string)
         (when-let (s (get-msg proc string 2))
           (unless (eq (aref s 0) ?s) (error "Bad status"))
           (funcall
            (cond
             ((eq (compare-strings s 1 7 "named:" nil nil) t)
              (let* ((nlen (derl--read-uint 2 s 7))
                     (name (substring s 9 (+ 9 nlen)))
                     (creation (derl--read-uint 4 s (+ 9 nlen))))
                (message "Name: %s, creation: %s" name creation)
                (process-put proc 'name (intern name))
                (process-put proc 'creation creation)
                (set-process-filter proc #'recv-challenge)))
             ;; TODO alive and case 3B)
             (t (error "Unknown status: %S" s)))
            proc "")))
       (recv-challenge (proc string)
         (when-let (s (get-msg proc string 2))
           (cl-assert (eq (aref s 0) ?N)) ; recv_challenge_reply tag
           (let* ((challenge-b (derl--read-uint 4 s 9))
                  (creation-b (derl--read-uint 4 s 13))
                  (nlen (derl--read-uint 2 s 17))
                  (name-b (substring s 19 (+ 19 nlen)))
                  (challenge-a (random (ash 1 32))) ; #x100000000
                  (digest (derl--gen-digest challenge-b cookie)))
             (process-put proc 'name-b (intern name-b))
             (process-put proc 'creation-b creation-b)
             (process-send-string
              proc (concat [0 21 ; Length
                              ?r] ; send_challenge_reply tag
                           (derl--write-uint 4 challenge-a)
                           digest))
             (set-process-filter proc #'recv-challenge-ack))))
       (recv-challenge-ack (proc string)
         (unless (eq (aref string 2) ?a) (error "Bad tag"))
         (let ((digest (substring string 3 (+ 3 16))))
           (message "Got challenge ACK: digest %S!" digest)
           (set-process-filter proc #'connected-filter)))
       (connected-filter (proc string)
         (when-let (s (get-msg proc string 4))
           (if (string= s "")
               (process-send-string proc "\0\0\0\0") ; Zero-length heartbeat
             (cl-assert (eq (aref s 0) 112)) ; Check that type is pass through
             (with-temp-buffer
               (set-buffer-multibyte nil)
               (insert s)
               (goto-char (1+ (point-min)))
               (let ((control-msg
                      (progn (cl-assert (eq (char-after) derl-ext-version))
                             (forward-char)
                             (derl-read)))
                     (msg (progn (cl-assert (eq (char-after) derl-ext-version))
                                 (forward-char)
                                 (derl-read))))
                 (message "Parsed: %S" (cons control-msg msg))
                 (pcase control-msg
                   ;; SEND_SENDER
                   (`[22 ,_from-pid [,_ pid ,_name ,to-pid ,_serial ,_creation]]
                    (derl-send to-pid msg))))))
           (connected-filter proc ""))))
    (let ((proc (make-network-process
                 :name "erl-emacs" :host host :service port
                 :coding 'raw-text :filter-multibyte nil :filter #'recv-status
                 :plist (list 'buf "")))
          (flags
           (eval-when-compile
             (derl--write-uint
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
      ;; Send send_name message
      (process-send-string
       proc (concat (derl--write-uint 2 (+ 15 (string-bytes name))) ; Length
                    [?N] ; send_name tag
                    flags
                    [0 0 0 0] ; Creation
                    (derl--write-uint 2 (string-bytes name)) ; Nlen
                    name))
      proc)))

(defun derl--offload-process (proc)
  (make-thread
   (lambda ()
     (set-process-thread proc (current-thread))
     (while (eq (process-status proc) 'open)
       (accept-process-output proc)))))

;; (cl-loop for process being the hash-values of derl--processes do
;;          (derl-send (derl-process-id process) 1337))

;; (setq derl-conn (derl-connect 'local (derl-epmd-port-please "arnie") "IYQBVJUETYMWBBGPADPN"))
;; (delete-process derl-conn)
;; (derl--offload-process derl-conn)

;; (message "Result of RPC: %S" (derl-rpc derl-conn 'erlang 'node))

(defun derl-self (conn)
  "Return an Erlang PID object for this process on the node connection CONN."
  (let ((name (process-get conn 'name))
        (id (derl-process-id derl--self))
        (serial 0)
        (creation (process-get conn 'creation)))
    `[,derl-tag pid ,name ,id ,serial ,creation]))

(defun derl-rpc (conn module function &rest args)
  "Apply FUNCTION in MODULE to ARGS on the remote node over CONN."
  (let* ((name-b (process-get conn 'name-b))
         (pid (derl-self conn))
         (control-msg (derl-term-to-binary `[6 ,pid nil rex])) ; REG_SEND
         (msg (derl-term-to-binary
               ;; {Who, {call, M, F, A, GroupLeader}}
               `[,pid [call ,module ,function ,args ,pid]])))
    (process-send-string
     conn
     (concat
      (derl--write-uint 4 (+ 1 (string-bytes control-msg) (string-bytes msg)))
      [112] ; Pass through
      control-msg msg)))
  (derl-receive (`[rex ,x] x)))

(require 'ert)

(ert-deftest derl-gen-digest-test ()
  (should (equal (derl--gen-digest #xb0babeef "kaka")
                 "\327k1\f\326ck'\344\263m\C-F\305P\C-KP")))

(ert-deftest derl-read-test ()
  (should (eq (derl-binary-to-term 131 97 #xff) 255))
  (should (eq (derl-binary-to-term 131 98 #xff #xff #xfc #x18) -1000))
  (should (equal (derl-binary-to-term 131 104 3 97 1 97 2 97 3) [1 2 3]))
  (should (eq (derl-binary-to-term 131 106) ()))
  (should (eq (derl-binary-to-term 131 98 255 255 255 255) -1)))

(ert-deftest derl-write-test ()
  (should (equal (derl-term-to-binary -1) "\203b\377\377\377\377"))
  (should (equal (derl-term-to-binary (- #x80000000)) "\203b\200\0\0\0"))

  (should
   (equal 
    (derl-term-to-binary [rex "hej"])
    "\203h\C-bd\0\C-crexk\0\C-chej"
    ;; (unibyte-string 131 104 2 100 0 3 114 101 120 107 0 3 104 101 106)
    )))

(ert-deftest derl-ext-roundtrip-test ()
  (should (let ((x [rex "node"]))
            (equal (derl-binary-to-term (derl-term-to-binary x)) x))))

(ert-deftest derl-selective-receive-test ()
  (let* (result
         (pid (derl-spawn
               (lambda () (derl-receive (3 t)) (setq result derl--mailbox)))))
    (derl-send pid 1)
    (derl-send pid 2)
    (derl-send pid 3)
    (thread-join (derl-process-thread (gethash pid derl--processes)))
    (should (equal result '(1 2)))))
