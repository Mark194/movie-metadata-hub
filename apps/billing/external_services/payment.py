import requests

from common.settings import get_settings

settings = get_settings()

async def process_payment(order_id: str, amount: float, payment_method: str):
    url = f'{settings.billing.payment_gateway_url}/pay'
    data = {'order_id': order_id, 'amount': amount, 'payment_method': payment_method}
    resp = requests.post(url, json=data, timeout=settings.billing.payment_timeout)
    resp.raise_for_status()
    return resp.json()