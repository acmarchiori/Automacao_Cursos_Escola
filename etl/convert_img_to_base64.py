import os
import base64
from zipfile import ZipFile
from bs4 import BeautifulSoup


def converter_imagens_para_base64(html_path, image_folder_path):
    """
    Converte todas as imagens em um arquivo HTML para base64 e substitui as
    referências por dados base64 no próprio HTML.
    """
    with open(html_path, 'r', encoding='utf-8') as file:
        soup = BeautifulSoup(file, 'html.parser')
    # Para cada tag de imagem no HTML
    for img_tag in soup.find_all('img'):
        img_src = img_tag.get('src', '')
        # Ignorar imagens externas (URLs)
        if img_src.startswith('http'):
            continue
        # Construir o caminho completo da imagem
        img_file_path = os.path.join(
            image_folder_path,
            os.path.basename(img_src)
        )
        if os.path.exists(img_file_path):
            # Detectar extensão da imagem
            ext = os.path.splitext(img_file_path)[-1][1:]
            # Ler e converter a imagem para base64
            with open(img_file_path, 'rb') as img_file:
                img_base64 = base64.b64encode(img_file.read()).decode('utf-8')
            # Atualizar o atributo src da imagem com o formato base64
            img_tag['src'] = f"data:image/{ext};base64,{img_base64}"
            print(f"Imagem {img_file_path} convertida para base64.")
        else:
            print(f"Imagem não encontrada: {img_file_path}")

    # Retornar o conteúdo HTML com as imagens convertidas
    return str(soup)


def processar_arquivos_zip(pasta_zip):
    """
    Processa todos os arquivos .zip em uma pasta, converte as imagens para
    base64 nos arquivos HTML descompactados e salva o HTML atualizado.
    """
    for root, _, files in os.walk(pasta_zip):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                # Criar uma pasta temporária para extrair o conteúdo do zip
                temp_folder = os.path.splitext(zip_path)[0]
                with ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_folder)
                # Identificar o arquivo HTML e a pasta de imagens
                html_file = None
                image_folder = None
                for sub_root, sub_dirs, sub_files in os.walk(temp_folder):
                    for sub_file in sub_files:
                        if sub_file.endswith('.html'):
                            html_file = os.path.join(sub_root, sub_file)
                        elif sub_file.startswith('image'):
                            image_folder = sub_root
                if html_file and image_folder:
                    print(f"Processando HTML: {html_file}")
                    # Converte imagens para base64 e salva o HTML atualizado
                    html_content_com_base64 = converter_imagens_para_base64(
                      html_file, image_folder
                    )
                    # Sobrescreve o HTML original com o conteúdo atualizado
                    with open(html_file, 'w', encoding='utf-8') as file:
                        file.write(html_content_com_base64)
                    print(
                        f"HTML atualizado com imagens em base64 "
                        f"salvo em {html_file}."
                        )
                else:
                    print(
                        f"Erro: Arquivo HTML ou pasta de imagens não "
                        f"encontrado em {temp_folder}"
                        )


# Caminho para a pasta contendo os arquivos .zip com HTML e imagens
pasta_zip = "/home/acmarchiori/Área de Trabalho/HTLM_LITERATURA"
processar_arquivos_zip(pasta_zip)
