import requests

client_secret = "1rqukbir5qip7e2dinfgo00ulis3v5d1ei0m93cvunlauhqd4lgl"

auth_response = requests.post(
    'https://sa-east-1fhwoijefj.auth.sa-east-1.amazoncognito.com/oauth2/token',
    data=f"grant_type=client_credentials&client_id=2c81kl8hsfam195k14bclv4ch&client_secret={client_secret}&scope=default-m2m-resource-server-eltpcl/read",
    headers={'Content-Type': 'application/x-www-form-urlencoded'}
)
print(auth_response.json())

# exit()
# Extrai o token da resposta
token = auth_response.json()['access_token']
# {
#     "access_token": "<token>",
#     "expires_in": 3600, 
#     "token_type": "Bearer"
# }

# Monta o cabeçalho com o token Bearer
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# payload em JSON
payload = {
  "tipo": 115,
  "timestamp": "08/03/2025 11:00",
  "animais": 53,
  "suplPorAnimal": 3960,
  "tratosPorDia": 2,
  "tipoSupl": 60,
  "fimCiclo": "25/6/2025",
  "diasTrato": 109,
  "horarios": [
    "15:00",
    "10:00"
  ]
}
# 'https://fscpkfxjy7.execute-api.sa-east-1.amazonaws.com/default/motar_payload'
# Faz a requisição para sua API
api_response = requests.post(
    'https://fscpkfxjy7.execute-api.sa-east-1.amazonaws.com/default/hello',
    headers=headers,
    json=payload  # O requests vai automaticamente converter o dict para JSON
)

# Imprime a resposta
print(api_response.json())