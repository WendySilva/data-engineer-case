import requests
from logging import Logger
from typing import Optional


class DataRequests:
    """
    Classe responsável por realizar requisições HTTP GET para obter arquivos binários (como arquivos `.parquet`) a partir de uma URL.

    Atributos
    ---------
    logger : Logger
        Instância de logger utilizada para registrar erros durante a execução.

    url : str
        URL do recurso que será requisitado.

    Métodos
    -------
    get_response() -> Optional[bytes]
        Realiza uma requisição HTTP GET à URL fornecida.
        Retorna o conteúdo em bytes se a resposta for bem-sucedida, ou None em caso de erro.

    Exemplo de uso
    --------------
    >>> import logging
    >>> logger = logging.getLogger(__name__)
    >>> url = "https://example.com/dados.parquet"
    >>> requester = DataRequests(logger=logger, url=url)
    >>> conteudo = requester.get_response()
    """

    def __init__(self, logger: Logger, url: str) -> None:
        self.logger = logger
        self.url = url

    def get_response(self) -> Optional[bytes]:
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            response = response.content
            return response
        except Exception as e:
            self.logger.error(f"Erro na requisição: {e}")