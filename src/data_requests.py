import requests
from logging import Logger

class DataRequests:
  """
  DataRequests

  Classe responsável por realizar requisições HTTP para obter arquivos binários (como arquivos `.parquet`), a partir de uma URL.

  Atributos:
  ----------
  logger : Logger
      Instância de logger utilizada para registrar erros durante a execução.

  url : str
      URL do recurso que será requisitado.

  Métodos:
  --------
  get_response() -> bytes
      Realiza uma requisição HTTP GET à URL fornecida. Caso a resposta seja bem-sucedida, retorna o conteúdo da resposta em bytes.
      Em caso de falha, registra o erro no logger e retorna None.

  Exemplo de uso:
  ---------------
  logger = logging.getLogger(__name__)
  url = "https://example.com/dados.parquet"
  requester = DataRequests(logger=logger, url=url)
  conteudo = requester.get_response()
  """
  def __init__(self,
               logger:Logger,
               url:str) -> None:
    self.logger = logger
    self.url = url

  def get_response(self):
    try:
      response = requests.get(self.url)
      response.raise_for_status()
      response = response.content
      return response
    except Exception as e:
      self.logger.error(f"Erro na requisição: {e}")