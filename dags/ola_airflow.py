from airflow.sdk import dag, task
import pendulum

@dag(
    schedule=None,                # roda só quando você manda
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["estudo"],
)
def ola_airflow():

    @task
    def cumprimentar():
        print("Olá, Airflow! Minha primeira task rodou 🎉")
        return "ok"

    cumprimentar()

ola_airflow()