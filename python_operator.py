"""Function to transform data"""
def transform():
    def latino_transform():
        pass

    def kennedy_transform():
        pass

    latino_transform()
    kennedy_transform()

transform_data=PythonOperator(task_id="transform", python_callable=transform)