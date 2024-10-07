# Deployment

PS1='\u@\h:\$ '

## Создаем deployment

kubectl apply -f 

Проверяем список replicaset, для этого выполним команду:

kubectl get replicaset


kubectl get pod

## заходим внутрь контейнера

kubectl exec -it my-deployment-7b766b679b-4vz46 -- bash

Ищем файлы nginx в /usr/share/nginx/html
Или просто смотрим переменные окружения контейнера:

kubectl exec -it my-deployment-7b766b679b-4vz46 -- env


## Чистим за собой кластер


kubectl delete deployment --all

