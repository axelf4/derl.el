;;; derl-tests.el  -*- lexical-binding: t -*-

(require 'ert)
(require 'derl)

(defun derl-binary-to-term (&rest args)
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (apply #'insert args)
    (goto-char (point-min))
    (unless (eq (get-byte) derl-ext-version) (error "Bad version"))
    (forward-char)
    (derl-read)))

(defun derl-term-to-binary (term)
  (with-temp-buffer
    (set-buffer-multibyte nil)
    (insert derl-ext-version)
    (derl-write term)
    (buffer-substring-no-properties (point-min) (point-max))))

(ert-deftest derl-gen-digest-test ()
  (should (equal (derl--gen-digest #xb0babeef "kaka")
                 "\327k1\f\326ck'\344\263m\C-F\305P\C-KP")))

(ert-deftest derl-read-test ()
  (should (eq (derl-binary-to-term 131 97 #xff) 255))
  (should (eq (derl-binary-to-term 131 98 #xff #xff #xfc #x18) -1000))
  (should (equal (derl-binary-to-term 131 104 3 97 1 97 2 97 3) [1 2 3]))
  (should (equal (derl-binary-to-term 131 108 0 0 0 1 106 106) '(nil)))
  (should (eq (derl-binary-to-term 131 98 255 255 255 255) -1)))

(ert-deftest derl-write-test ()
  (should (equal (derl-term-to-binary -1) "\203b\377\377\377\377"))
  (should (equal (derl-term-to-binary (- #x80000000)) "\203b\200\0\0\0"))
  (should (equal (derl-term-to-binary '(nil)) "\203l\0\0\0\1jj"))
  (should (equal (derl-term-to-binary [rex "ok"]) "\203h\2w\3rexm\0\0\0\2ok"))

  (should (equal (derl-term-to-binary 1.1337) "\203F?\362#\242\234w\232k")))

(ert-deftest derl-ext-roundtrip-test ()
  (dolist (x `((0 . "x") [atom 0.1] [,derl-tag nil]))
    (should (equal (derl-binary-to-term (derl-term-to-binary x)) x))))

(ert-deftest derl-reference-unique-test ()
  (should-not (equal (derl-make-ref) (derl-make-ref))))

(ert-deftest derl-name-registry-test ()
  (unwind-protect
      (progn (derl-register 'foo (derl-self))
             (should (equal (derl-whereis 'foo) (derl-self))))
    (derl-register 'foo nil))
  (should (null (derl-whereis 'foo))))

(ert-deftest derl-selective-receive-test ()
  (let* (result
         (pid (derl-spawn
               (iter-make (derl-receive (2)) (setq result derl--mailbox)))))
    (dotimes (i 3) (! pid i))
    (while (null result) (derl--run))
    (should (equal result '(0 1)))))

(ert-deftest derl-exit-test ()
  (let ((pid (derl-spawn (iter-make (derl-receive (_))))))
    (derl-exit pid 'kill)
    (while (gethash pid derl--processes) (derl--run))))

(ert-deftest derl-timeout-test ()
  (should (eq (derl-receive :after 0 'timeout) 'timeout)))

;; Local Variables:
;; read-symbol-shorthands: (("!" . "derl-send"))
;; End:
