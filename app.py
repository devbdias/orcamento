import streamlit as st
from main import main
import time

# Variável de controle para verificar se o ETL está em andamento
etl_em_andamento = False

# Função para executar o ETL
def executar_etl():
    global etl_em_andamento
    etl_em_andamento = True
    progresso = st.empty()  # Espaço vazio para mostrar o progresso
    start_time = time.time()  # Captura o tempo inicial

    with st.spinner('Executando ETL...'):
        main()

    end_time = time.time()  # Captura o tempo final
    tempo_total = end_time - start_time  # Calcula o tempo total de execução
    st.success(f'ETL concluído em {tempo_total:.2f} segundos!')
    
    etl_em_andamento = False

# Criando o botão e atribuindo a função a ele
if st.button("Rodar ETL Orçamento"):
    if not etl_em_andamento:
        executar_etl()
    else:
        st.warning('O ETL já está em andamento. Pressione o botão novamente para interromper.')
