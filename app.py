from flask import Flask, render_template, request, url_for, redirect, flash
from flask import send_file
#from flask_mysqldb import MySQL
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_
import sqlalchemy
import getpass
import pandas as pd

app = Flask(__name__,template_folder='templates')

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://lhyvtnau:z0hg6VUFZjtC-Ee9XG4Do6Xd9yi9D9Zc@drona.db.elephantsql.com:5432/lhyvtnau'

db = SQLAlchemy(app)

#settings
app.secret_key = 'mysecretkey'

termino = ''

def ejecucionClientes(conn, rutaClientes):
    df_grupo_imputacion = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_GRUPO_IMPUTACION_DETALLE"', conn)
    df_moneda = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_MONEDA_DETALLE"', conn)
    df_lista_precios = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_LISTA_DE_PRECIOS_DETALLE"', conn)
    df_region = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_REGION_DETALLE"', conn)
    df_grupo_precios = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_GRUPO_PRECIOS_DETALLE"', conn)
    df_idioma = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_IDIOMA_DETALLE"', conn)
    df_direccion = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_DIRECCION_DETALLE"', conn)
    df_igv = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_IGV_DETALLE"', conn)
    df_poblacion = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_POBLACION_DETALLE"', conn)
    df_distrito = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_DISTRITO_DETALLE"', conn)
    df_email = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_EMAIL_DETALLE"', conn)
    df_grupo_clientes = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_GRUPO_CLIENTES_DETALLE"', conn)
    df_cod_postal = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_COD_POSTAL_DETALLE"', conn)
    df_zona_transporte = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_ZONA_TRANSPORTE_DETALLE"', conn)
    df_telefono = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_TELEFONO_DETALLE"', conn)
    df_dni_ruc = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_DNI_RUC_DETALLE"', conn)
    df_nombre = pd.io.sql.read_sql('SELECT * FROM "CLIENT_TOTAL_NOMBRE_DETALLE"', conn)

    writer = pd.ExcelWriter(rutaClientes, engine='xlsxwriter')

    df_grupo_imputacion.to_excel(writer, sheet_name='GrupoImputacion', startrow=3)
    df_moneda.to_excel(writer, sheet_name='Moneda', startrow=3)
    df_lista_precios.to_excel(writer, sheet_name='ListaPrecios', startrow=3)
    df_region.to_excel(writer, sheet_name='Region', startrow=3)
    df_grupo_precios.to_excel(writer, sheet_name='GrupoPrecios', startrow=3)
    df_idioma.to_excel(writer, sheet_name='Idioma', startrow=3)
    df_direccion.to_excel(writer, sheet_name='Direccion', startrow=3)
    df_igv.to_excel(writer, sheet_name='IGV', startrow=3)
    df_poblacion.to_excel(writer, sheet_name='Poblacion', startrow=3)
    df_distrito.to_excel(writer, sheet_name='Distrito', startrow=3)
    df_email.to_excel(writer, sheet_name='Email', startrow=3)
    df_grupo_clientes.to_excel(writer, sheet_name='GrupoClientes', startrow=3)
    df_cod_postal.to_excel(writer, sheet_name='CodPostal', startrow=3)
    df_zona_transporte.to_excel(writer, sheet_name='ZonaTransporte', startrow=3)
    df_telefono.to_excel(writer, sheet_name='Telefono', startrow=3)
    df_dni_ruc.to_excel(writer, sheet_name='DNI - RUC', startrow=3)
    df_nombre.to_excel(writer, sheet_name='Nombre', startrow=3)

    writer.save()

    del df_grupo_imputacion, df_moneda, df_lista_precios, df_region, df_grupo_precios, df_idioma, df_direccion, df_igv
    del df_poblacion, df_email, df_grupo_clientes, df_cod_postal, df_zona_transporte, df_telefono, df_dni_ruc, df_nombre

    rsClientes = 'Listo'
    #return send_file(rutaClientes, as_attachment=True)

def ejecucionProveedores(conn, rutaProveedores):
    pd_poblacion = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_POBLACION_DETALLE"', conn)
    pd_region = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_REGION_DETALLE"', conn)
    pd_raz_social = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_RAZON_SOCIAL_DETALLE"', conn)
    pd_idioma = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_IDIOMA_DETALLE"', conn)
    pd_ind_reten = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_INDICADOR_RETENCION_DETALLE"', conn)
    pd_clase_interloc = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_CLASE_INTERLOCUTOR_DETALLE"', conn)
    pd_cod_postal = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_CODIGO_POSTAL_DETALLE"', conn)
    pd_dni_ruc = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_DNI_RUC_DETALLE"', conn)
    pd_street = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_STREET_DETALLE"', conn)
    pd_pers_nat = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_PERSONA_NATURAL_DETALLE"', conn)
    pd_sociedad_gl = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_SOCIEDAD_GL_DETALLE"', conn)
    pd_grup_cuenta = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_GRUPO_CUENTA_DETALLE"', conn)
    pd_estado_contrib = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_ESTADO_CONTRIBUYENTE_DETALLE"', conn)
    pd_email = pd.io.sql.read_sql('SELECT * FROM "PROV_TOTAL_EMAIL_DETALLE"', conn)
    conn.close()
    writer = pd.ExcelWriter(rutaProveedores, engine='xlsxwriter')

    pd_poblacion.to_excel(writer, sheet_name='Poblacion', startrow=3)
    pd_region.to_excel(writer, sheet_name='Region', startrow=3)
    pd_raz_social.to_excel(writer, sheet_name='RazSocial', startrow=3)
    pd_idioma.to_excel(writer, sheet_name='Idioma', startrow=3)
    pd_ind_reten.to_excel(writer, sheet_name='IndcadorRetencion', startrow=3)
    pd_clase_interloc.to_excel(writer, sheet_name='ClaseInterlocutor', startrow=3)
    pd_cod_postal.to_excel(writer, sheet_name='CodPotal', startrow=3)
    pd_dni_ruc.to_excel(writer, sheet_name='Dni_Ruc', startrow=3)
    pd_street.to_excel(writer, sheet_name='Direccion', startrow=3)
    pd_pers_nat.to_excel(writer, sheet_name='PersNatural', startrow=3)
    pd_sociedad_gl.to_excel(writer, sheet_name='SociadadGL', startrow=3)
    pd_grup_cuenta.to_excel(writer, sheet_name='GrupoCuenta', startrow=3)
    pd_estado_contrib.to_excel(writer, sheet_name='EstadoContrib', startrow=3)
    pd_email.to_excel(writer, sheet_name='Email', startrow=3)

    writer.save()

    del pd_poblacion, pd_region, pd_raz_social, pd_idioma, pd_ind_reten, pd_clase_interloc, pd_cod_postal, pd_dni_ruc, pd_street
    del pd_pers_nat, pd_sociedad_gl, pd_grup_cuenta, pd_estado_contrib, pd_email

def ejecucionMateriales(conn, rutaMateriales):
    pd_caractPlanif = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CARACT_PLANIFICACION_DETALLE"', conn)
    pd_matDenom = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_DENOMINACION_DETALLE"', conn)
    pd_ambValoracion = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_AMBITO_DE_VALORACION_DETALLE"', conn)
    pd_catValoracion = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CATEGORIA_DE_VALORACION_DETALLE"', conn)
    pd_aprovEspecial = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_APROVISIONAMIENTO_ESPECIAL_DETALLE"', conn)
    pd_grupProd2 = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_GRUPO_PRODUCTOS2_DETALLE"', conn)
    pd_identImpu = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_IDENTIFICADOR_DE_IMPUESTOS_DETALLE"', conn)
    pd_grupCompras = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_GRUPO_DE_COMPRAS_DETALLE"', conn)
    pd_clasFiscCompra = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CLASIFICACION_FISCAL_COMPRA_DETALLE"', conn)
    pd_clasFiscVenta2 = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CLASIFICACION_FISCAL_VENTA2_DETALLE"', conn)
    pd_aprov = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_APROVISIONAMIENTO_DETALLE"', conn)
    pd_clasFiscVenta1 = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CLASIFICACION_FISCAL_VENTA1_DETALLE"', conn)
    pd_orgVenta = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_ORGANIZACION_VENTAS_DETALLE"', conn)
    pd_jerarProdCompra = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_JERARQUIA_DE_PRODUCTOS_COMPRAS_DETALLE"', conn)
    pd_canalDist = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CANAL_DE_DISTRIBUCION_DETALLE"', conn)
    pd_jerarProdVenta = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_JERARQUIA_DE_PRODUCTOS_VENTAS_DETALLE"', conn)
    pd_tipVal = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_TIPO_DE_VALORACION_DETALLE"', conn)
    pd_centBenef = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CENTRO_DE_BENEFICIO_DETALLE"', conn)
    pd_almAprovExtr = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_ALMACEN_APROVIS_EXTERNO_DETALLE"', conn)
    pd_grupProd4 = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_GRUPO_PRODUCTOS4_DETALLE"', conn)
    pd_PlanifNece = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_PLANIFICACION_NECESIDADES_DETALLE"', conn)
    pd_unidadMedida = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_UNIDAD_DE_MEDIDA_DETALLE"', conn)
    pd_grupProd1 = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_GRUPO_PRODUCTOS1_DETALLE"', conn)
    pd_centro = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CENTRO_DETALLE"', conn)
    pd_conPrecios = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_CONTROL_DE_PRECIOS_DETALLE"', conn)
    pd_grupArti = pd.io.sql.read_sql('SELECT * FROM "MAT_TOTAL_GRUPO_ARTICULOS_DETALLE"', conn)

    writer = pd.ExcelWriter(rutaMateriales, engine='xlsxwriter')

    pd_caractPlanif.to_excel(writer, sheet_name='CaractPlanificacion', startrow=3)
    pd_matDenom.to_excel(writer, sheet_name='MatDescripcion', startrow=3)
    pd_ambValoracion.to_excel(writer, sheet_name='AmbitoValoracion', startrow=3)
    pd_catValoracion.to_excel(writer, sheet_name='CategoriaValoracion', startrow=3)
    pd_aprovEspecial.to_excel(writer, sheet_name='AprovEspecial', startrow=3)
    pd_grupProd2.to_excel(writer, sheet_name='GrupoProductos2', startrow=3)
    pd_identImpu.to_excel(writer, sheet_name='IdentifImpuestos', startrow=3)
    pd_grupCompras.to_excel(writer, sheet_name='GrupoCompras', startrow=3)
    pd_clasFiscCompra.to_excel(writer, sheet_name='ClasiFisCompra', startrow=3)
    pd_clasFiscVenta2.to_excel(writer, sheet_name='ClasiFisVenta2', startrow=3)
    pd_aprov.to_excel(writer, sheet_name='Aprovisionamiento', startrow=3)
    pd_clasFiscVenta1.to_excel(writer, sheet_name='ClasiFisVenta1', startrow=3)
    pd_orgVenta.to_excel(writer, sheet_name='OrganizacionVenta', startrow=3)
    pd_jerarProdCompra.to_excel(writer, sheet_name='JerarquiaProduct', startrow=3)
    pd_canalDist.to_excel(writer, sheet_name='CanalDistribucion', startrow=3)
    pd_jerarProdVenta.to_excel(writer, sheet_name='JerarquiaProductoVenta', startrow=3)
    pd_tipVal.to_excel(writer, sheet_name='TipoValoracion', startrow=3)
    pd_centBenef.to_excel(writer, sheet_name='CentroBeneficio', startrow=3)
    pd_almAprovExtr.to_excel(writer, sheet_name='AlmacenAproviExterno', startrow=3)
    pd_grupProd4.to_excel(writer, sheet_name='GrupoProductos4', startrow=3)
    pd_PlanifNece.to_excel(writer, sheet_name='PlanificacionNecesidad', startrow=3)
    pd_unidadMedida.to_excel(writer, sheet_name='UnidadMedida', startrow=3)
    pd_grupProd1.to_excel(writer, sheet_name='GrupoProductos1', startrow=3)
    pd_centro.to_excel(writer, sheet_name='Centro', startrow=3)
    pd_conPrecios.to_excel(writer, sheet_name='ControlPrecios', startrow=3)
    pd_grupArti.to_excel(writer, sheet_name='GrupoArticulos', startrow=3)

    writer.save()

    del pd_caractPlanif, pd_matDenom, pd_ambValoracion, pd_catValoracion, pd_aprovEspecial, pd_grupProd2, pd_identImpu
    del pd_grupCompras, pd_clasFiscCompra, pd_clasFiscVenta2, pd_aprov, pd_clasFiscVenta1, pd_orgVenta, pd_jerarProdCompra
    del pd_canalDist, pd_jerarProdVenta, pd_tipVal, pd_centBenef, pd_almAprovExtr, pd_grupProd4, pd_PlanifNece, pd_unidadMedida
    del pd_grupProd1, pd_centro, pd_conPrecios, pd_grupArti

    rsMateriales = 'Listo'

def ejecucionListaMateriales(conn, rutaListaMateriales):
    pd_uniMedida = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_UNIDAD_MEDIDA_DETALLE"', conn)
    pd_docModif = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_DOCUMENTO_MODIFICACION_DETALLE"', conn)
    pd_uniMedBase = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_UNIDAD_MEDIDA_BASE_DETALLE"', conn)
    pd_listMat = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_LISTA_MAT_DETALLE"', conn)
    pd_material = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_MATERIAL_DETALLE"', conn)
    pd_cantSolici = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_CANTIDAD_SOLICITADA_DETALLE"', conn)
    pd_cantBase = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_CANTIDAD_BASE_DETALLE"', conn)
    pd_componente = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_COMPONENTE_DETALLE"', conn)
    pd_alternativa = pd.io.sql.read_sql('SELECT * FROM "LIST_MATERIAL_M_ALTERNATIVA_DETALLE"', conn)

    writer = pd.ExcelWriter(rutaListaMateriales, engine='xlsxwriter')

    pd_uniMedida.to_excel(writer, sheet_name='UnidadMedida', startrow=3)
    pd_docModif.to_excel(writer, sheet_name='DocumentoModificacion', startrow=3)
    pd_uniMedBase.to_excel(writer, sheet_name='UnidadMedidaBase', startrow=3)
    pd_listMat.to_excel(writer, sheet_name='ListaMateriales', startrow=3)
    pd_material.to_excel(writer, sheet_name='Material', startrow=3)
    pd_cantSolici.to_excel(writer, sheet_name='CantidadSolicitada', startrow=3)
    pd_cantBase.to_excel(writer, sheet_name='CantidadBase', startrow=3)
    pd_componente.to_excel(writer, sheet_name='Componente', startrow=3)
    pd_alternativa.to_excel(writer, sheet_name='Alternativa', startrow=3)

    writer.save()

    del pd_uniMedida, pd_docModif, pd_uniMedBase, pd_listMat, pd_material, pd_cantSolici
    del pd_cantBase, pd_componente, pd_alternativa

    rsListaMateriales = 'Listo'

def ejecucionVersionFabricacion(conn, rutaVersionFabricacion):
    pd_utiliza = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_UTILIZACION_DETALLE"', conn)
    pd_contador = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_CONTADOR_DETALLE"', conn)
    pd_verid = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_VERID_DETALLE"', conn)
    pd_material = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_MATERIAL_DETALLE"', conn)
    pd_hojaRuta = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_HOJARUTA_DETALLE"', conn)
    pd_TipoHRuta = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_TIPO_HRUTA_DETALLE"', conn)
    pd_Alternativa = pd.io.sql.read_sql('SELECT * FROM "VERS_FABRICACION_ALTERNATIVA_DETALLE"', conn)

    writer = pd.ExcelWriter(rutaVersionFabricacion, engine='xlsxwriter')

    pd_utiliza.to_excel(writer, sheet_name='Utilizacion', startrow=3)
    pd_contador.to_excel(writer, sheet_name='Contador', startrow=3)
    pd_verid.to_excel(writer, sheet_name='Verid', startrow=3)
    pd_material.to_excel(writer, sheet_name='Material', startrow=3)
    pd_hojaRuta.to_excel(writer, sheet_name='HojaRuta', startrow=3)
    pd_TipoHRuta.to_excel(writer, sheet_name='TipoHRuta', startrow=3)
    pd_Alternativa.to_excel(writer, sheet_name='Alternativa', startrow=3)

    writer.save()

    del pd_utiliza, pd_contador, pd_verid, pd_material, pd_hojaRuta, pd_TipoHRuta, pd_Alternativa

    rsVersFabricacion = 'Listo'

def ejecucionReceta(conn, rutaReceta):
    pd_gpo = pd.io.sql.read_sql('SELECT * FROM "RECETA_GPO_HOJARUTA_DETALLE"', conn)
    pd_mat = pd.io.sql.read_sql('SELECT * FROM "RECETA_MATERIAL_DETALLE"', conn)
    pd_conta = pd.io.sql.read_sql('SELECT * FROM "RECETA_CONTADOR_DETALLE"', conn)
    pd_workCenter = pd.io.sql.read_sql('SELECT * FROM "RECETA_WORK_CENTER_DETALLE"', conn)
    pd_tiemProceso = pd.io.sql.read_sql('SELECT * FROM "RECETA_TIEMPO_PROCESO_DETALLE"', conn)
    pd_operFase = pd.io.sql.read_sql('SELECT * FROM "RECETA_OPERAC_FASE_DETALLE"', conn)
    pd_claveControl = pd.io.sql.read_sql('SELECT * FROM "RECETA_CLAVE_CONTROL_DETALLE"', conn)
    pd_claveModelo = pd.io.sql.read_sql('SELECT * FROM "RECETA_CLAVE_MODELO_DETALLE"', conn)
    pd_docModCabecera = pd.io.sql.read_sql('SELECT * FROM "RECETA_DOC_MOD_CABECERA_DETALLE"', conn)

    writer = pd.ExcelWriter(rutaReceta, engine='xlsxwriter')

    pd_gpo.to_excel(writer, sheet_name='GPO HojaRuta', startrow=3)
    pd_mat.to_excel(writer, sheet_name='Material', startrow=3)
    pd_conta.to_excel(writer, sheet_name='Contador', startrow=3)
    pd_workCenter.to_excel(writer, sheet_name='WorkCenter', startrow=3)
    pd_tiemProceso.to_excel(writer, sheet_name='TiempoProceso', startrow=3)
    pd_operFase.to_excel(writer, sheet_name='OperacFase', startrow=3)
    pd_claveControl.to_excel(writer, sheet_name='ClaveControl', startrow=3)
    pd_claveModelo.to_excel(writer, sheet_name='ClaveModelo', startrow=3)
    pd_docModCabecera.to_excel(writer, sheet_name='DocModCabecera', startrow=3)

    writer.save()

    del pd_gpo, pd_mat, pd_conta, pd_workCenter, pd_tiemProceso, pd_operFase, pd_claveControl, pd_claveModelo, pd_docModCabecera

    rsRecetas = 'Listo'

def ejecucionPIR(conn, rutaPIR):
    pd_pirCantBase = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_CANTIDAD_BASE_CABECERA_DETALLE"', conn)
    pd_pirPrecioNetoCab = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_PRECIO_NETO_CABECERA_DETALLE"', conn)
    pd_pirControlConfir = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_CONTROL_DE_CONFIRMACION_DETALLE"', conn)
    pd_pirPrecioNetCondiciones = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_PRECIO_NETO_CONDICIONES_DETALLE"', conn)
    pd_pirPlazoEntPrev = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_PLAZO_ENTREGA_PREVISTO_DETALLE"', conn)
    pd_pirCodMatDet = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_CODIGO_MATERIAL_DETALLE"', conn)
    pd_pirCantBaseCondi = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_CANTIDAD_BASE_CONDICIONES_DETALLE"', conn)
    pd_pirIndImpu = pd.io.sql.read_sql('SELECT * FROM "PIR_TOTAL_INDICADOR_DE_IMPUESTOS_DETALLE"', conn)
    writer = pd.ExcelWriter(rutaPIR, engine='xlsxwriter')
    pd_pirCantBase.to_excel(writer, sheet_name='CantidadBaseCabecera', startrow=3)
    pd_pirPrecioNetoCab.to_excel(writer, sheet_name='PrecioNetoCabecera', startrow=3)
    pd_pirControlConfir.to_excel(writer, sheet_name='ControlConfirmacion', startrow=3)
    pd_pirPrecioNetCondiciones.to_excel(writer, sheet_name='PrecioNetoCondiciones', startrow=3)
    pd_pirPlazoEntPrev.to_excel(writer, sheet_name='PlazoEntregaPrevisto', startrow=3)
    pd_pirCodMatDet.to_excel(writer, sheet_name='CodMaterial', startrow=3)
    pd_pirCantBaseCondi.to_excel(writer, sheet_name='CantidadBaseCondiciones', startrow=3)
    pd_pirIndImpu.to_excel(writer, sheet_name='IndicadorImpuestos', startrow=3)
    writer.save()
    del pd_pirCantBase, pd_pirPrecioNetoCab, pd_pirControlConfir, pd_pirPrecioNetCondiciones, pd_pirPlazoEntPrev, pd_pirCodMatDet
    del pd_pirCantBaseCondi, pd_pirIndImpu
    rsPIR = 'Listo'
    #return send_file(rutaPIR, as_attachment=True)

def ejecucionActivoFijo(conn, rutaActivoFijo):
    actFij_pd_Catag = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_CATALOGO_DETALLE"', conn)
    actFij_pd_Centro = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_CENTRO_DETALLE"', conn)
    actFij_pd_CentroCosto = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_CENTRO_DE_COSTO_DETALLE"', conn)
    actFij_pd_Clase = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_CLASE_ACTIVO_FIJO_DETALLE"', conn)
    actFij_pd_Denom1 = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_DENOMINACION1_DETALLE"', conn)
    actFij_pd_Denom2 = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_DENOMINACION2_DETALLE"', conn)
    actFij_pd_SocieGL = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_SOCIEDADGL_DETALLE"', conn)
    actFij_pd_Sociedad = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_SOCIEDAD_DETALLE"', conn)
    actFij_pd_Status = pd.io.sql.read_sql('SELECT * FROM "ACT_FIJO_TOTAL_STATUS_DETALLE"', conn)
    writer = pd.ExcelWriter(rutaActivoFijo, engine='xlsxwriter')
    actFij_pd_Catag.to_excel(writer, sheet_name='Catalogo', startrow=3)
    actFij_pd_Centro.to_excel(writer, sheet_name='Centro', startrow=3)
    actFij_pd_CentroCosto.to_excel(writer, sheet_name='CentroCosto', startrow=3)
    actFij_pd_Clase.to_excel(writer, sheet_name='Clase', startrow=3)
    actFij_pd_Denom1.to_excel(writer, sheet_name='Denominacion1', startrow=3)
    actFij_pd_Denom2.to_excel(writer, sheet_name='Denominacion2', startrow=3)
    actFij_pd_SocieGL.to_excel(writer, sheet_name='SociedadGL', startrow=3)
    actFij_pd_Sociedad.to_excel(writer, sheet_name='Sociedad', startrow=3)
    actFij_pd_Status.to_excel(writer, sheet_name='Status', startrow=3)
    writer.save()
    del actFij_pd_Catag, actFij_pd_Centro, actFij_pd_CentroCosto, actFij_pd_Clase, actFij_pd_Denom1, actFij_pd_Denom2
    del actFij_pd_SocieGL, actFij_pd_Sociedad, actFij_pd_Status

class analyticsmail(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    url = db.Column(db.String(255))
    visita = db.Column(db.Integer)
    fecha = db.Column(db.DateTime, default= datetime.now)

    def __init__(self, url):
        self.id = 3
        self.url = url
        self.visita = 1

class powerbi(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    ambiente = db.Column(db.String(255))
    url = db.Column(db.String(500))

    def __init__(self, ambiente, url):
        self.id = 1
        self.ambiente = ambiente
        self.url = url

    def __repr__(self):
        return f"<id={self.id}, ambiente={self.ambiente}, url={self.url}>"

class consultas_glosario(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    termino = db.Column(db.String(255))
    fecha_busqueda = db.Column(db.DateTime, default = datetime.now)

    def __init__(self, termino):
        self.id = 1
        self.termino = termino

class usuario_logeado(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    usuario = db.Column(db.String(255))
    fecha_login = db.Column(db.DateTime, default=datetime.now)

    def __init__(self, usuario):
        self.id= 1
        self.usuario = usuario

class glosario(db.Model):

    id = db.Column(db.Integer, primary_key=True)
    termino = db.Column(db.String(255))
    definicion = db.Column(db.String(255))
    campo1 = db.Column(db.String(255))
    campo2 = db.Column(db.String(255))
    campo3 = db.Column(db.String(255))
    campo4 = db.Column(db.String(255))

    def __init__(self, termino, definicion, campo1, campo2, campo3, campo4):
        self.termino  = termino
        self.definicion = definicion
        self.campo1 = campo1
        self.campo2 = campo2
        self.campo3 = campo3
        self.campo4 = campo4

    def __repr__(self):
        return f"<id={self.id}, termino={self.termino}, definicion={self.definicion}>"

@app.route('/')
def index():
    #getWindowsUsername()
    #usrLog = usuario_logeado(usuario=getWindowsUsername())
    #db.session.add(usrLog)
    #db.session.commit()
    return render_template('index.html')


@app.route('/Inicio')
def inicio():
    return render_template('index.html')

@app.route('/Metadatos')
def metadatos():
    return render_template('Metadatos.html')

@app.route('/Calidad')
def calidad():
    return render_template('Calidad.html')

@app.route('/CalidadBAU')
def calidadBAU():
    pbi_update = powerbi.query.filter_by(ambiente='BAU').first()
    return render_template('Calidad.html', url = pbi_update.url)

@app.route('/Prueba')
def prueba():
    return render_template('prueba.html')

@app.route('/Equipo')
def equipo():
    return render_template('Equipo.html')

@app.route('/CalidadDetalle')
def calidadPrueba():
    return render_template('CalidadDetalle.html')

@app.route('/CalidadPRX')
def calidadPRD():
    return render_template('.html')

@app.route('/CalidadFenix')
def calidadFenix():
    pbi_update = powerbi.query.filter_by(ambiente='Fenix').first()
    return render_template('CalidadFenix.html', url = pbi_update.url)

@app.route('/PowerBI')
def updatePowerBI():
    return render_template('PowerBIGyGD.html')

@app.route('/update', methods=['POST'])
def updateLinkPowerBI():
    if request.method == 'POST':
        numP = request.form['posPower']
        url = request.form['urlPower']
        if (numP == '1'):
            numP = 'BAU'
            pbi_update = powerbi.query.filter_by(ambiente = numP).update({ "url" : url })
            db.session.commit()
            return render_template('PowerBIGyGD.html', mensaje='Actualizado BAU')
        elif (numP == '2'):
            numP = 'Fenix'
            pbi_update = powerbi.query.filter_by(ambiente=numP).update({"url": url})
            db.session.commit()
            return render_template('PowerBIGyGD.html', mensaje='Actualizado Fenix')
        else:
            return render_template('PowerBIGyGD.html', mensaje='No Actualizado')

@app.route('/Cultura')
def Cultura():
    envioMail = analyticsmail("noticia3")
    db.session.add(envioMail)
    db.session.commit()
    return redirect("https://hbr.org/2020/05/is-your-business-masquerading-as-data-driven")

@app.route('/maestrosDetalle', methods=['POST'])
def maestrosDetalle():
    if request.method == 'POST':
        engine = sqlalchemy.create_engine("postgres://rjwgbaci:5kekO5OMLoZ8KzGuQrrdo21xOTxfR-pQ@drona.db.elephantsql.com:5432/rjwgbaci")
        conn = engine.connect()

        rsClientes, rsProveedores, rsMateriales, rsListaMateriales = '','','',''
        rsRecetas, rsVersFabricacion, rsPIR, rsActivoFijo = '','','',''

        rutaClientes = r"archivos\Clientes.xlsx"
        rutaProveedores = r"archivos\Proveedores.xlsx"
        rutaMateriales = r"archivos\Materiales.xlsx"
        rutaListaMateriales = r"archivos\ListaMateriales.xlsx"
        rutaReceta = r"archivos\Recetas.xlsx"
        rutaVersionFabricacion = r"archivos\VersFabricacion.xlsx"
        rutaPIR = r"archivos\PIR.xlsx"
        rutaActivoFijo = r"archivos\ActivoFijo.xlsx"

        if request.form.get('demo-clientes'):
            rsClientes = request.form['demo-clientes']
            if rsClientes == 'on':
                ejecucionClientes(conn,rutaClientes)
                return send_file(rutaClientes, as_attachment=True)

        if request.form.get('demo-proveedores'):
            rsProveedores = request.form['demo-proveedores']
            if rsProveedores == 'on':
                ejecucionProveedores(conn,rutaProveedores)
                rsProveedores = 'Listo'
                return send_file(rutaProveedores, as_attachment=True)

        if request.form.get('demo-materiales'):
            rsMateriales = request.form['demo-materiales']
            if rsMateriales == 'on':
                ejecucionMateriales(conn,rutaMateriales)
                return send_file(rutaMateriales, as_attachment=True)

        if request.form.get('demo-listaMateriales'):
            rsListaMateriales = request.form['demo-listaMateriales']
            if rsListaMateriales == 'on':
                ejecucionListaMateriales(conn, rutaListaMateriales)
                return send_file(rutaListaMateriales, as_attachment=True)

        if request.form.get('demo-recetas'):
            rsRecetas = request.form['demo-recetas']
            if rsRecetas == 'on':
                ejecucionReceta(conn, rutaReceta)
                return send_file(rutaReceta, as_attachment=True)

        if request.form.get('demo-versFabricacion'):
            rsVersFabricacion = request.form['demo-versFabricacion']
            if rsVersFabricacion == 'on':
                ejecucionVersionFabricacion(conn, rutaVersionFabricacion)
                return send_file(rutaVersionFabricacion, as_attachment=True)

        if request.form.get('demo-PIR'):
            rsPIR = request.form['demo-PIR']
            if rsPIR == 'on':
                ejecucionPIR(conn, rutaPIR)
                return send_file(rutaPIR, as_attachment=True)

        if request.form.get('demo-ActivoFijo'):
            rsActivoFijo = request.form['demo-ActivoFijo']
            if rsActivoFijo == 'on':
                ejecucionActivoFijo(conn, rutaActivoFijo)
                return send_file(rutaActivoFijo, as_attachment=True)

        query = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'rjwgbaci' AND pid = pg_backend_pid()"
        conn.execute(query);
        conn.close()

        return render_template('CalidadDetalle.html', status = 'Listo')

@app.route('/search_term', methods=['POST'])
def search_term():
    if request.method == 'POST':
        fullname = request.form['termino']
        consulta_glo = consultas_glosario(termino=fullname)
        db.session.add(consulta_glo)
        db.session.commit()

        flash('Búsqueda realizada: ' + str(fullname))

        fullname = fullname.upper()
        search = "%{}%".format(fullname)
        all_glosario = glosario.query.filter(or_(glosario.termino.ilike(search),glosario.definicion.ilike(search))).all()

        return render_template('Metadatos.html', contacts = all_glosario)

if __name__ == '__main__':
    app.run(debug=True)
