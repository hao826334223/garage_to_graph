# Argo Workflow
**Install Argo Workflows**<br>
```
bash argo_init.sh
```
**To make the UI available in localhost**<br>
```
kubectl -n argo port-forward deployment/argo-server 2746:2746
```
![](../images/argo-workflow-ui.png)