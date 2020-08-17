from google.cloud import bigquery


class BigQuery:

    def __init__(self, payload):
        self.payload = payload
        self.client = bigquery.Client()

    def create_dataset_if_not_exists(self, dataset_name):
        """
        Check BigQuery if a given name dataset exists.
        If not exists, create a new dataset with the given dataset_name.
        :param dataset_name: The new Dataset name to create in BigQuery
        """
        dataset_id = f"{self.client.project}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = 'US'

        try:
            dataset = self.client.create_dataset(dataset)
            print("Created dataset {}.{}".format(
                self.client.project, dataset.dataset_id))
        except Exception as err:
            # The named dataset already exists.
            print(f'Dataset named {dataset_name} currently exists!')
            pass

