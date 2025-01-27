import azure.functions as func
import logging
from services.sql_operations_datamart import Crear_Modelo_Estrella, Carga_Modelo_Estrella

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="ETLLoadStage")
def ETLLoadStage(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        
        try:
            Crear_Modelo_Estrella()
        except Exception as e:
            logging.error(f"Error inesperado al crear/verificar la tabla: {e}")
            return func.HttpResponse(f"Error inesperado al crear/verificar la tabla: {e}", status_code=500)

        try:
            Carga_Modelo_Estrella()
        except Exception as e:
            logging.error(f"Error inesperado al crear/verificar la tabla: {e}")
            return func.HttpResponse(f"Error inesperado al crear/verificar la tabla: {e}", status_code=500)

        return func.HttpResponse("Carga al modelo estrella exitosa.", status_code=200)
         
    except Exception as e:
        logging.error(f"Error inesperado en el proceso: {e}")
        return func.HttpResponse(f"Error inesperado en el proceso: {e}", status_code=500)