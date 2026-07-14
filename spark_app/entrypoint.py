import logging

from spark_app.common.app_factory import AppFactory

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


def main():
    app = AppFactory().build()
    app.execute()

if __name__ == "__main__":
    main()