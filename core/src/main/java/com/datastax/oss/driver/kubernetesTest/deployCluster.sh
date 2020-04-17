kubectl apply -f cassandra-service.yaml
# get service
kubectl get svc cassandra

kubectl apply -f cassandra-statefulset.yaml
# get nodes
kubectl get statefulset cassandra
kubectl get pods -l="app=cassandra"
kubectl exec -it cassandra-0 -- nodetool status

# deploy load balancer
# on mac to make the load balancer expose external ip
minikube tunnel
kubectl apply -f loadBalancer.yaml
