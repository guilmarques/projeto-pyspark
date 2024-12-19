# Teste Processamento de Dados de Viagens

Este projeto implementa um pipeline de processamento de dados utilizando PySpark. Ele é estruturado com conceitos de Programação Orientada a Objetos (POO) e foi projetado para ser compatível com diversas plataformas, incluindo Kubernetes e ambientes Linux.

## Estrutura do Projeto

```
projeto-pyspark/
├── Dockerfile                # Imagem Docker para execução do pipeline
├── app/                      # Diretório principal do código
│   ├── main.py               # Ponto de entrada do pipeline
│   ├── processor.py          # Classe EventProcessor
│   ├── aggregator.py         # Classe Aggregator
│   ├── writer.py             # Classe Writer
│   └── utils.py              # Funções auxiliares (se necessário)
├── data/                     # Diretório para arquivos de entrada e saída
├── tests/                    # Diretório contendo os testes unitários
│   ├── test_processor.py     # Testes para EventProcessor
│   ├── test_aggregator.py    # Testes para Aggregator
│   └── test_writer.py        # Testes para Writer
│   └── conftest.py           # Arquivo de configuração comum para todos os testes
├── requirements.txt          # Dependências do projeto
├── spark-submit.sh           # Script para submissão em clusters Kubernetes
├── README.md                 # Documentação do projeto
```

---

## Ferramentas Necessárias

### Pré-requisitos
1. **Python**: Certifique-se de ter o Python (>=3.8) instalado.
2. **Docker**:
   - **Instalar no Linux**:
     ```bash
     sudo apt update
     sudo apt install -y docker.io
     sudo systemctl start docker
     sudo systemctl enable docker
     ```
   - **Instalar no Windows/Mac**: Baixe o [Docker Desktop](https://www.docker.com/products/docker-desktop) e siga as instruções de instalação.
3. **Java**: Caso opte por testar local sem docker, certifique-se de ter o Java JDK 8 ou superior instalado (necessário para PySpark).
4. **Kubernetes** (opcional, para execução em cluster):
   - Instale o `kubectl` seguindo a [documentação oficial](https://kubernetes.io/docs/tasks/tools/).

---

## Configuração do Ambiente

1. **Clone o repositório**:
   ```bash
   git clone <link-do-repositorio>
   cd projeto-pyspark
   ```

2. **Build da Imagem Docker**:
   ```bash
   docker build -t pyspark-pipeline:latest .
   ```

3. **(Opcional caso queira testar sem Docker com spark instalado na máquina local) Instale as dependências Python**:
   Crie um ambiente virtual e instale as dependências do projeto.
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # No Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

---

## Como Executar o Pipeline Localmente

1. **Executar com Docker**:
   Certifique-se que o comando $(pwd) do Linux seja substituído por um equivalente caso esteja em outro sistema operacional, ou apenas copie e cole hard code o caminho do seu projeto.
   ```bash
   docker run --rm -v $(pwd)/data:/app/data pyspark-pipeline:latest
   ```
   Caso prefira pode interagir com o container entrando nele e executando o comando **python3 main.py** logo após entrar. Para entrar no container utilize:
   ```bash
   docker run -it --entrypoint /bin/bash -v $(pwd)/data:/app/data pyspark-pipeline:latest
   ```
   
2. **(Opcional) Executar com Kubernetes**:
   - Publique a imagem Docker para um registro (ex.: Docker Hub):
     ```bash
     docker tag pyspark-pipeline:latest <seu-usuario>/pyspark-pipeline:latest
     docker push <seu-usuario>/pyspark-pipeline:latest
     ```
   - Use o script `spark-submit.sh` para submeter o job:
     ```bash
     ./spark-submit.sh
     ```

3. **(Caso optou por executar sem docker) Executar com Python**:
   ```bash
   python main.py
   ```

---

## Testes Unitários

1. **Executar Testes**:
   
    **1.1** No Docker, utilize o comando
   ```bash
   docker run --rm --entrypoint pytest -v $(pwd)/data:/app/data pyspark-pipeline:latest tests/
   ```
   Ou entre no container executando os dois comandos em ordem:
     ```bash
     docker run -it --entrypoint /bin/bash -v $(pwd)/data:/app/data pyspark-pipeline:latest
     pytest tests/
     ```
   **1.2** (Caso opte por ambiente local) No ambiente local:
     ```bash
     pytest tests/
     ```

2. **Cobertura de Testes**:
   
    **2.1** No Docker, utilize o comando
   ```bash
   docker run --rm --entrypoint pytest -v $(pwd)/data:/app/data pyspark-pipeline:latest --cov=app tests/
   ```
   Ou entre no container executando os dois comandos em ordem:
     ```bash
     docker run -it --entrypoint /bin/bash -v $(pwd)/data:/app/data pyspark-pipeline:latest
     pytest --cov=app tests/
     ```
   **2.2** (Caso opte por ambiente local) Para verificar a cobertura dos testes no ambiente local, execute:
      ```bash
        pytest --cov=app tests/
      ```

---

## Considerações Importantes

1. Sobre o código:

- O código está contemplando exatamente o que o enunciado pede, lendo o input.json e gerando o arquivo parquet de saída, dá para fazer a leitura das estruturas usando streaming com o Kafka por exemplo usando o readStream(), dá para ler a partir de uma fila do SQS por exemplo, onde mostra quais arquivos json chegaram em ordem, bem como quais não foram processados ainda e com isso o código lê cada um dos arquivos que estão na fila nomeados e etc.
- No processor.py eu fiz o método que foi pedido para filtrar os dados como mencionado, porém eu entendi que no método process_events da classe Processor não era para retornar o dataframe com o filtro para as agregacoes, portanto o método de filtrar ficou sem uso, deixei no código para verificarem que funciona, mas não coloco ele no return do método (Vide comentário na linha 49 do arquivo processor.py).
- O input_data.json deve ficar dentro da pasta /data para o código ler com sucesso. Os resultados ficam na pasta data/output (coloquei um dataframe para o dado processado enriquecido particionando pelo estado origem e destino e mais outros 3 de agregações geradas pelo metodo agregator separados pelas respectivas pastas).

2. Fluxo de Execução:
Durante o desenvolvimento, use Docker localmente.
Caso queira fazer deploy em produção com o kubernetes por exemplo:
- Construa e publique a imagem Docker (utilizar a imagem que está no projeto).
- Configurar Kubernetes (exemplo: clusters EKS, GKE ou AKS).
- Substituir no arquivo "spark-submit.sh" os valores <kubernetes-api-url> e <docker-image-url> pelos valores do seu cluster e imagem.
- Submeta o job usando o spark-submit.sh.

3. Armazenamento de Dados Para facilitar a compatibilidade:

- Volumes no Docker para leitura/escrita de dados localmente.
- No Kubernetes, configurar Persistent Volumes (PV) para armazenamento compartilhado entre pods.
