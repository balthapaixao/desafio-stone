# desafio-stone
Repositorio para desafio Stone

## Passo 1
rodar no terminal sudo docker-compose up -d
para poder subir os containers do docker. Os containers do Postgres, Airflow e PGAdmin estão conectadas por meio de uma rede.

## Passo 2
Entrar no airflow pelo localhost:8080 como descrito no container e dar trigger nos dois DAGS (IBGE e Stone) pois no momento o codigo para o airflow rodar diariamente está comentado e é necessário um trigger para começar a rodá-lo

### Airflow
Dentro do airflow, para os dois DAGs são rodados um processo de ETL.

## Passo 3
Após fazer a carga dos dados no postgres é feita por meio de um jupyter notebook a conexão com as bases para serem executadas as queries necessárias para que as analises sejam feitas.
