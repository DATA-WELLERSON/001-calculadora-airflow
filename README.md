# 001-calculadora-airflow

Projeto bobinho pra aprender o básico do básico do Airflow 3.x.

A ideia é simples: uma DAG que recebe dois números e uma operação, decide qual task rodar usando `@task.branch`, executa a conta e mostra o resultado. Nada de produção, nada de complexidade — só o suficiente pra entender como as peças se encaixam.

## O que tem aqui

| DAG | O que faz |
|-----|-----------|
| `ola_airflow` | Hello world. Uma task, só pra ver o negócio rodar. |
| `calculadora` | Soma, subtrai, multiplica ou divide dois números via Params. |

### Conceitos cobertos pela `calculadora`

- **TaskFlow API** — `@dag` e `@task` em vez de operadores legados
- **`@task.branch`** — decide qual caminho executar em runtime
- **XCom automático** — dados passando entre tasks sem boilerplate
- **Params** — configuração passada via UI no "Trigger DAG w/ config"
- **`trigger_rule="none_failed_min_one_success"`** — pra task final não ser pulada quando as outras 3 ficam skipped

## Como rodar

Precisa ter Docker e Docker Compose instalados.

```bash
# Sobe o ambiente
docker compose up -d

# Acessa a UI
# http://localhost:8080  →  usuário: airflow  |  senha: airflow
```

Depois é só habilitar uma das DAGs na UI, clicar em **Trigger DAG w/ config** e passar os parâmetros:

```json
{
  "a": 10,
  "b": 5,
  "operacao": "somar"
}
```

Operações válidas: `somar`, `subtrair`, `multiplicar`, `dividir`.

## Estrutura

```
dags/
  ola_airflow.py   # hello world
  calculadora.py   # projeto principal
docker-compose.yaml
```
