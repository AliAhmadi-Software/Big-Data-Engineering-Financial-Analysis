kubectl exec spark-master-88f78d74c-s6stx -it -- \pyspark --conf spark.driver.bindAddress=10.244.0.17 --conf spark.driver.host=10.244.0.17
#Stream-processing JOB
kubectl apply -f spark-job.yaml
kubectl get jobs
kubectl logs job/spark-stream-processing

# roll
kubectl apply -f role.yaml
kubectl apply -f rolebinding.yaml

kubectl get rolebindings -n default -o yaml
kubectl get rolebindings -n default

kubectl apply -f configmap-manager-role.yaml
kubectl apply -f configmap-manager-binding.yaml


