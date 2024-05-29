import prefect

def log(message) -> None:
    """Logs the operations

    Args:
        message (_type_): The messages on the functions.
    """
    prefect.context.logger.info(f'\n {message}')