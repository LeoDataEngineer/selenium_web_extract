import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options


def extract_text(id, url, by_type, identifier):
    # Configurar el servicio para ChromeDriver
    service = Service(ChromeDriverManager().install())
    
    # Configurar opciones de Chrome
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")

    # Iniciar el navegador
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        # Navegar a la página web
        print(f"Navegando a la URL: {url}")
        driver.get(url)
        
        # Hacer scroll hacia abajo para cargar el contenido si es necesario
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        
        # Esperar hasta que el contenedor de los productos esté presente
        wait = WebDriverWait(driver, 10)  # Incrementar el tiempo de espera
        print(f"Esperando el elemento con {by_type} y {identifier}")
        wait.until(EC.presence_of_element_located((by_type, identifier)))

        # Extraer datos utilizando el tipo de localización y el identificador proporcionado
        element = driver.find_element(by_type, identifier)
        print(f"Elemento encontrado: {element}")

        # Obtener el texto del elemento
        text = element.text
        print(f"Texto encontrado para {id}: {text}")
        text_name = (id, text)
        
        return text_name
    except Exception as e:
        print(f"Error al extraer texto para {id} con URL {url}: {e}")
        return None
    finally:
        # Asegurarse de que el navegador se cierra
        driver.quit()


columnas = ['id', 'Empresa', 'producto', 'link', 'xpath']

links_product = [
    ['1', 'Jumbo', 'azucar', 'https://www.jumbo.com.ar/azucar/azucar-y-edulcorantes?map=ft,sub-categoria', '//*[@id="gallery-layout-container"]/div[2]/section/a/article/div[5]/div/div/div/div[1]/div/span/div/div'],
    ['2', 'Jumbo', 'arroz', 'https://www.jumbo.com.ar/arroz/arroz?map=ft,sub-categoria', '//*[@id="gallery-layout-container"]/div[17]/section/a/article/div[5]/div/div/div/div[1]/div/span/div/div'],
    ['3', 'Coto', 'azucar', 'https://www.cotodigital3.com.ar/sitios/cdigi/browse/catalogo-almac%C3%A9n-endulzantes-az%C3%BAcar/_/N-1w1x9xa?Dy=1&Nf=product.startDate%7CLTEQ%2B1.718928E12%7C%7Cproduct.endDate%7CGTEQ%2B1.718928E12&Nr=AND(product.sDisp_200%3A1004%2Cproduct.language%3Aespa%C3%B1ol%2COR(product.siteId%3ACotoDigital))', '//*[@id="divProductAddCart_sku00298412"]/div[1]/span/span'],
    ['4', 'disco', 'azucar', 'https://www.disco.com.ar/azucar?_q=azucar&map=ft', '/html/body/div[2]/div/div[1]/div/div[10]/div/div[2]/section/div[2]/div/div[4]/section/div/div/div/div/div[2]/div/div[3]/div/div/div/div/div[3]/section/a/article/div[5]/div/div/div/div[1]/div/span/div/div']
]
info = pd.DataFrame(links_product, columns=columnas)
info

# Se extrae la data de la web
productos = []
for index, row in info.iterrows():
    id = row['id']
    url = row['link']
    xpath = row['xpath']
    
    result = extract_text(id, url, By.XPATH, xpath)
    if result:  # Solo añadir si no es None
        productos.append(result)
    
    # Esperar 1 segundo antes de la siguiente iteración
    print("Esperando 1 segundos antes de la siguiente iteración...")
    time.sleep(1)

# Imprimir el diccionario resultante
print(productos)
    
# Crear el DataFrame
df = pd.DataFrame(productos, columns=['id', 'precio'])
print(df)

# Hacer el merge de los DataFrames en función de la columna 'id'
df_final = pd.merge(df, info, on='id')
# Nuevo orden de columnas
columnas_reordenadas = ['id', 'Empresa', 'producto', 'precio', 'link', 'xpath']

# Crear un nuevo DataFrame con las columnas reordenadas
df_final = df_final[columnas_reordenadas]
print(df_final)

def clean_price(price):
    # Eliminar el símbolo de dólar y los puntos de los miles
    clean_price = price.replace('$', '').replace('.', '').replace(',', '.')
    # Convertir a float
    return float(clean_price)

# Aplicar la función al DataFrame
df_final['precio'] = df_final['precio'].apply(clean_price)
df_final['id'] = df_final['id'].astype(int)

df_final.to_csv('productos.csv', index=False)
print(df_final)



