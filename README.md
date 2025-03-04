# Projeto Python MBA Engenharia de Dados

Este projeto foi desenvolvido como parte da disciplina de Python para Engenharia de Dados do MBA de Engenharia de Dados da Impacta (2024/2025). O objetivo é criar um pipeline de dados para a limpeza e tratamento de um arquivo CSV utilizando técnicas de manipulação de dados com a biblioteca Pandas e outras ferramentas.

## Descrição do Projeto

O pipeline de dados é dividido em duas etapas principais:

1. **Limpeza dos Dados (Raw -> Bronze)**:
   - Realizada pelo arquivo `DataClean.py`.
   - Remove valores nulos e dados inconsistentes.
   - Seleciona as colunas relevantes para análise.
   - Cria novas colunas com base em regras específicas.

2. **Tratamento dos Dados (Bronze -> Silver)**:
   - Realizada pelo arquivo `Transform.py`.
   - Adiciona novas colunas com base em regras de negócio.
   - Exemplos de transformações:
     - **Tempo de Voo**: Converte o tempo de voo de horas para minutos.
     - **Turno de Partida**: Classifica o horário de partida do voo em turnos (manhã, tarde, noite, madrugada).

O arquivo `main.py` consolida e executa as funções principais de limpeza e tratamento dos dados, além de padronizar o ambiente e a organização dos arquivos.

## Estrutura do Projeto
```
projeto_python_mba/
│
├── src/
│ ├── main.py
│ ├── DataClean.py
│ ├── Transform.py
│ └── utils/
│ └── helpers.py # Funções auxiliares, se necessário
│
├── data/
│ ├── raw/ # Dados brutos
│ ├── bronze/ # Dados após a limpeza (DataClean.py)
│ ├── silver/ # Dados após transformação (Transform.py)
│ └── gold/ # Dados finais (se necessário)
│
├── requirements.txt # Dependências do projeto
├── README.md # Documentação do projeto
└── .gitignore # Arquivos e pastas ignorados pelo Git
```


## Como Executar o Projeto

1. **Clone o repositório**:
   ```bash
   git clone https://github.com/seu-usuario/projeto-python-mba.git
   cd projeto-python-mba

2. **Clone o repositório**:
   ```bash
   git clone https://github.com/seu-usuario/projeto-python-mba.git
   cd projeto-python-mba

3. **Clone o repositório**:
   ```bash
   git clone https://github.com/seu-usuario/projeto-python-mba.git
   cd projeto-python-mba 


