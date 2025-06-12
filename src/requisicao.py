import requests
import io

class Requisicao:
  def __init__(self, url:str) -> None:
    self.url = url

  def get_response(self):
    try:
      response = requests.get(self.url)
      response.raise_for_status()
      response = self._bytes_buffer(response)
      return response
    except Exception as e:
      print(f"Erro na requisição: {e}")
    
  def _bytes_buffer(self, response):
    return io.BytesIO(response.content)
  