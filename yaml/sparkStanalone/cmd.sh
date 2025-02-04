$ kubectl cp test.py spark-pod:/tmp/
$ kubectl exec -it spark-pod -- spark-submit /tmp/test.py