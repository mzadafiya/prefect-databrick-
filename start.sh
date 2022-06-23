. ./env.sh

prefect server start --expose -d
nohup prefect agent local start &
prefect create project 'Covid Analysis'
prefect register --project "Covid Analysis" -p src/prefect/flow.py
