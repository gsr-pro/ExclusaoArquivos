ExcluiDestino_Turbo 🚀
O ExcluiDestino_Turbo é uma ferramenta em Python de alta performance projetada para a limpeza massiva de diretórios. Ele utiliza um modelo de concorrência produtor-consumidor para garantir que a exclusão de milhares de arquivos ocorra na velocidade máxima permitida pelo seu hardware (I/O e CPU).

✨ Funcionalidades
Arquitetura Turbo: Implementa um pipeline de ThreadPoolExecutor com Queue e os.scandir, superando scripts de exclusão sequenciais simples.

Auto-tuning: Ajusta automaticamente o número de threads com base nos núcleos da sua CPU (entre 4 e 32 workers).

Exclusão Segura (Wipe): Opção de sobrescrever arquivos com dados aleatórios (os.urandom) antes da remoção, dificultando a recuperação de dados.

Logs Inteligentes:

Console limpo com monitoramento de progresso (Heartbeat).

Geração automática de log de erros em formato CSV (thread-safe) para conferência posterior.

Robustez: Tenta forçar a exclusão alterando permissões de arquivos (chmod) caso encontre erros de acesso.

Limpeza Pós-Processamento: Identifica e remove pastas vazias recursivamente após a exclusão dos arquivos.

Modo de Segurança (Dry Run): Permite simular a operação sem apagar nada.

🛠️ Como Usar
Pré-requisitos: Ter o Python 3.6+ instalado. Não requer bibliotecas externas.

Configuração: Abra o script e ajuste as constantes no bloco CONFIGURAÇÕES GERAIS se necessário:

SECURE_PASSES: Defina como 1 para sobrescrever ou 0 para exclusão rápida.

DRY_RUN: Defina como True para testar antes de executar.

Execução:

Bash

python ExcluiDestino_Turbo.py
Confirmação: Informe o caminho do diretório e digite SIM quando solicitado.

⚠️ Aviso de Segurança
Este script foi desenhado para destruição de dados. Use-o com cautela.

A sobrescrita segura em SSDs pode não garantir 100% de irrecuperabilidade devido ao funcionamento do Wear Leveling dos controladores, mas adiciona uma camada extra de proteção.

Sempre verifique o caminho informado antes de confirmar a operação.

📊 Estrutura de Logs
Ao final da execução, se houver falhas (arquivos travados, permissões negadas, etc), uma pasta logs/ será criada no destino com um arquivo CSV contendo:

Timestamp

Etapa (scan/excluir)

Caminho do arquivo

Mensagem de erro detalhada
