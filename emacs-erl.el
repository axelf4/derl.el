;;; emacs-erl.el --- Erlang toolkit  -*- lexical-binding: t -*-

(require 'bindat)

;;; Erlang Port Mapper Daemon (EPMD) interaction

(defun epmd-port-please (name cb)
  "Get the distribution port of the node NAME."
  (let* ((epmd-port 4369)
         (port-please2-req-bindat-spec
          (bindat-type
            :pack-var s
            (length uint 16 :pack-val (1+ (string-bytes s)))
            (tag u8 :pack-val 122)
            (node-name str (1- length) :pack-val s)))
         (port2-resp-bindat-spec
          (bindat-type
            (tag u8 :pack-val 119)
            (result u8)
            (data
             . (if (/= result 0) (unit nil)
                 (struct
                  (portno uint 16)
                  (node-type u8)
                  (protocol u8)
                  (highest-version uint 16) (lowest-version uint 16)
                  (nlen uint 16) (node-name str nlen)
                  (elen uint 16) (extra vec elen u8))))))
         (proc
          (make-network-process
           :name "epmd-port-req" :host 'local :service epmd-port
           :coding 'raw-text :filter-multibyte nil :filter
           (lambda (proc string)
             (set-process-sentinel proc nil)
             (delete-process proc)
             (let* ((alist (bindat-unpack port2-resp-bindat-spec string))
                    (result (cdr (assq 'result alist))))
               (funcall cb (if (/= result 0) (cons 'error result)
                             (bindat-get-field alist 'data 'portno)))))
           :sentinel (lambda (proc event) (funcall cb (cons 'error event))))))
    (process-send-string proc (bindat-pack port-please2-req-bindat-spec name))
    proc))

(defun epmd-port-please-sync (name)
  (let* (result
         (proc (epmd-port-please name (lambda (x) (setq result x)))))
    (while (accept-process-output proc))
    result))

(epmd-port-please-sync "arnie")

;;; Erlang Distribution Protocol

(defconst erl-send-name-bindat-spec
  (bindat-type
    :pack-var alist
    (length uint 16 :pack-val (+ 15 (string-bytes (cdr (assq 'name alist)))))
    (_ u8 :pack-val ?N)
    (flags uint 64)
    (creation uint 32)
    (nlen uint 16 :pack-val (string-bytes (cdr (assq 'name alist))))
    (name str nlen)))

(defconst erl-recv-status-bindat-spec
  (bindat-type
    (length uint 16)
    (tag u8 :pack-val ?s)
    (status str (1- length))))

(defconst erl-recv-challenge-bindat-spec
  (bindat-type
    (length uint 16)
    (tag u8 :pack-val ?N)
    (flags uint 64)
    (challenge uint 32)
    (creation uint 32)
    (nlen uint 16)
    (name str nlen)))

(defconst erl-send-challenge-reply
  (bindat-type
    (length uint 16 :pack-val 21)
    (tag u8 :pack-val ?r)
    (challenge uint 32)
    (digest str 16)))

(defconst erl-recv-challenge-ack-bindat-spec
  (bindat-type
    (length uint 16)
    (tag u8 :pack-val ?a)
    (digest str 16)))

(defun erl-gen-digest (challenge cookie)
  "Generate a message digest (the gen_digest() function)."
  (cl-flet ((hexdigit-to-int (ch) (- ch (if (<= ?0 ch ?9) ?0 (- ?a 10)))))
    (cl-loop
     with s = (md5 (concat cookie (number-to-string challenge)))
     ;; Convert the hexadecimal string to binary
     for i below (length s) by 2
     collect (logior (ash (hexdigit-to-int (aref s i)) 4)
                     (hexdigit-to-int (aref s (1+ i))))
     into xs finally return (apply #'string xs))))

(cl-defun erl-conn (port cookie)
  (cl-labels
      ((recv-status
        (proc string)
        (let* ((alist (bindat-unpack erl-recv-status-bindat-spec string))
               (length (+ 2 (cdr (assq 'length alist)))))
          (pcase (cdr (assq 'status alist))
            ;; TODO alive and case 3B)
            ((and (pred (string-prefix-p "named:")) status)
             (let* ((nlen (logior (ash (aref status 6) 8) (aref status 7)))
                    (name (substring status 8 (+ 8 nlen)))
                    (creation (logior (ash (aref status (+ 8 nlen)) 24)
                                      (ash (aref status (+ 9 nlen)) 16)
                                      (ash (aref status (+ 10 nlen)) 8)
                                      (aref status (+ 11 nlen)))))
               (process-put proc 'name name)
               (process-put proc 'creation creation)
               (message "Name: %s, creation: %s" name creation)
               (set-process-filter proc #'recv-challenge)))
            (status (error "Unknown status: %S" status)))
          (when (< length (length string))
            (funcall (process-filter proc) proc (substring string length)))))
       (recv-challenge
        (proc string)
        (let* ((alist (bindat-unpack erl-recv-challenge-bindat-spec string))
               (b-challenge (cdr (assq 'challenge alist)))
               (a-challenge (random (ash 1 32))) ; #x100000000
               (digest (erl-gen-digest b-challenge cookie)))
          (process-send-string
           proc (bindat-pack erl-send-challenge-reply
                             `((challenge . ,a-challenge) (digest . ,digest))))
          (set-process-filter proc #'recv-challenge-ack)))
       (recv-challenge-ack
        (proc string)
        (let* ((alist (bindat-unpack erl-recv-challenge-ack-bindat-spec string)))
          (message "Got challenge ACK: %S!" alist)
          (set-process-filter proc #'connected-filter)))
       (connected-filter
        (proc string)
        (message "Received: %S" string)
        ;; Zero length messages are some kind of heartbeat?
        (when (string= string "\0\0\0\0") (process-send-string proc string))))
    (let ((proc (make-network-process
                 :name "erl-emacs" :host 'local :service port
                 :coding 'raw-text :filter-multibyte nil :filter #'recv-status
                 :sentinel (lambda (proc string) (message "Sentinel: %S" string))))
          (flags
           (eval-when-compile
             (logior
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
              (ash 1 32) ; DFLAG_SPAWN
              (ash 1 33) ; DFLAG_NAME_ME
              (ash 1 34) ; DFLAG_V4_NC
              #x1000000)))) ; DFLAG_HANDSHAKE_23
      (process-send-string
       proc (bindat-pack erl-send-name-bindat-spec
                         `((flags . ,flags) (creation . #x12345678)
                           (name . ,(system-name)))))
      proc)))

(setq erl-conn (erl-conn 42761 "IYQBVJUETYMWBBGPADPN"))
(delete-process erl-conn)
