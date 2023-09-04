;;; derl-tests.el  -*- lexical-binding: t -*-

(require 'ert)
(require 'derl)

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
  (let ((x [atom 0.1]))
    (should (equal (derl-binary-to-term (derl-term-to-binary x)) x))))

(ert-deftest derl-reference-unique-test ()
  (should-not (equal (derl-make-ref) (derl-make-ref))))

(ert-deftest derl-selective-receive-test ()
  (let* (result
         (pid (derl-spawn
               (iter-make (derl-receive (2)) (setq result derl--mailbox)))))
    (dotimes (i 3) (! pid i))
    (while (null result) (derl--scheduler-run))
    (should (equal result '(0 1)))))

(ert-deftest derl-exit-normal-test ()
  (let ((pid (derl-spawn (iter-make (derl-receive (_))))))
    (derl-exit pid 'normal)
    (while (gethash pid derl--processes) (derl--scheduler-run))))

(ert-deftest derl-timeout-test ()
  (should (eq (derl--call (iter-make (derl-receive (_))) 0) 'timeout)))

;; Local Variables:
;; read-symbol-shorthands: (("!" . "derl-send"))
;; End:
