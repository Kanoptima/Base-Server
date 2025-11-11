""" Database helper functions. """

import logging

from base_server.helpers.messaging import AutomationMessage
from app.models.clickup_task import ClickupTask
from app.models.client import Client
from app.tasks.keypay import get_business_id

logger = logging.getLogger(__name__)


def get_client_and_business(task: ClickupTask, results: list[AutomationMessage]) -> tuple[Client | None, str | None]:
    """Retrieve client and KeyPay business ID for the task."""
    client = Client.get_by_id(task.client_id)
    if not isinstance(client, Client):
        results.append(AutomationMessage.error(
            f'Client with id {task.client_id} not found.'))
        logger.error('Client with id %s not found.', task.client_id)
        return None, None
    if not client.payroll_name:
        results.append(AutomationMessage.error(
            f'No payroll authoriser set for {client}.'))
        logger.error('No payroll authoriser set for %s.', client)
        return None, None

    business_id = get_business_id(client)
    if not business_id:
        results.append(AutomationMessage.error(
            f'Keypay business not found for {client}.'))
        logger.error('Keypay business not found for %s.', client)
        return None, None

    return client, business_id
