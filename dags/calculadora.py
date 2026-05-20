"""
DAG Calculadora — exemplo didático de branching com TaskFlow API.

Recebe `a`, `b` e `operacao` via Params (você passa pela UI quando
clica em "Trigger DAG with config"), escolhe qual task rodar e
mostra o resultado.
"""
from airflow.sdk import dag, task
from airflow.exceptions import AirflowSkipException
import pendulum


@dag(
    dag_id="calculadora",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["estudo", "exemplo"],
    params={
        "a": 10,
        "b": 5,
        "operacao": "somar",   # somar | subtrair | multiplicar | dividir
    },
    doc_md=__doc__,
)
def calculadora():

    # ---------- 1. Lê e valida entrada ----------
    @task
    def ler_entrada(**context) -> dict:
        params = context["params"]
        a = float(params["a"])
        b = float(params["b"])
        op = params["operacao"].lower().strip()

        operacoes_validas = {"somar", "subtrair", "multiplicar", "dividir"}
        if op not in operacoes_validas:
            raise ValueError(f"Operação inválida: {op}. Use uma de {operacoes_validas}")

        # Aqui o if/else "de negócio" que você queria treinar:
        if op == "dividir" and b == 0:
            raise ValueError("Não dá pra dividir por zero, amigo.")

        print(f"Entrada lida: a={a}, b={b}, operacao={op}")
        return {"a": a, "b": b, "op": op}

    # ---------- 2. Branching: decide qual task rodar ----------
    @task.branch
    def escolher_op(entrada: dict) -> str:
        # Retorna o task_id da próxima task que deve rodar.
        # As outras serão automaticamente SKIPPED.
        return entrada["op"]   # "somar", "subtrair", etc — bate com os task_ids abaixo

    # ---------- 3. As 4 operações ----------
    @task(task_id="somar")
    def somar(entrada: dict) -> float:
        r = entrada["a"] + entrada["b"]
        print(f"{entrada['a']} + {entrada['b']} = {r}")
        return r

    @task(task_id="subtrair")
    def subtrair(entrada: dict) -> float:
        r = entrada["a"] - entrada["b"]
        print(f"{entrada['a']} - {entrada['b']} = {r}")
        return r

    @task(task_id="multiplicar")
    def multiplicar(entrada: dict) -> float:
        r = entrada["a"] * entrada["b"]
        print(f"{entrada['a']} * {entrada['b']} = {r}")
        return r

    @task(task_id="dividir")
    def dividir(entrada: dict) -> float:
        r = entrada["a"] / entrada["b"]
        print(f"{entrada['a']} / {entrada['b']} = {r}")
        return r

    # ---------- 4. Mostra o resultado ----------
    # trigger_rule="none_failed_min_one_success" é a chave:
    # roda quando pelo menos uma upstream rodou com sucesso (e nenhuma falhou).
    # Sem isso, essa task seria pulada porque 3 das 4 upstreams ficam "skipped".
    @task(trigger_rule="none_failed_min_one_success")
    def mostrar_resultado(s=None, sub=None, m=None, d=None):
        resultado = next((v for v in (s, sub, m, d) if v is not None), None)
        print(f"🧮  Resultado final: {resultado}")
        return resultado

    # ---------- Monta o fluxo ----------
    entrada = ler_entrada()
    escolha = escolher_op(entrada)

    s   = somar(entrada)
    sub = subtrair(entrada)
    m   = multiplicar(entrada)
    d   = dividir(entrada)

    # Dependência: o branch decide quem roda dentre as 4
    escolha >> [s, sub, m, d]

    mostrar_resultado(s, sub, m, d)


calculadora()
