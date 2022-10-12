def publish_messages(project_id: str, topic_id: str) -> None:
    """Publishes multiple messages to a Pub/Sub topic."""
    from airflow import models
    from google.cloud import pubsub_v1
    from receiver import receive_messages

    PROJECT_ID = models.Variable.get("PROJECT_ID")
    sub_id = models.Variable.get("IDR_receive_messages_play")

    publisher = pubsub_v1.PublisherClient()
    ''' The `topic_path` method creates a fully qualified identifier
        in the form `projects/{project_id}/topics/{topic_id}`
    '''
    topic_path = publisher.topic_path(project_id, topic_id)
    message_list = receive_messages(PROJECT_ID, sub_id)

    for i in message_list:
        data = i.data
        # When you publish a message, the client returns a future.
        future = publisher.publish(topic_path, data)
        print(future.result())