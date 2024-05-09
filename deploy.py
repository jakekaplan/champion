import modal

app = modal.App("scrape-data-app")


@app.function(schedule=modal.Period(minutes=1))
def scrape_data():
    print("Scraped data I swear...")


@app.local_entrypoint()
def main():
    scrape_data.remote()


if __name__ == '__main__':
    modal.runner.deploy_app(app)
