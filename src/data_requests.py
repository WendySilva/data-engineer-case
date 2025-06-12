import requests

class DataRequests:
  def __init__(self, url:str) -> None:
    self.url = url

  def get_response(self):
    try:
      response = requests.get(self.url)
      response.raise_for_status()
      response = response.content
      return response
    except Exception as e:
      print(f"Erro na requisição: {e}")