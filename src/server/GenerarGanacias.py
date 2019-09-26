import cx_Oracle as ORA

QUERY = 'update per_ganancia_liq set importe_escala = {escala},ganancia_acum = {ganancia} where legajo = {legajo} and anio = 2019 and mes = 8;\n'
QUERYJULIO = 'select legajo,importe_escala,ganancia_acum from per_ganancia_liq where anio = 2019 and mes = 7'
QUERYAGOSTO = 'select legajo,ganancia,ganancia from per_ganancia_liq where anio = 2019 and mes = 8'

SALIDA = 'ganancia.sql'

def obtenerDatos():

    con,cur = conectar()

    cur.execute(QUERYJULIO)
    resJulio = cur.fetchall()

    cur.execute(QUERYAGOSTO)
    resAgosto = cur.fetchall()

    datosJulio = {}

    for res in resJulio:
        datosJulio[res[0]] = [res[1],res[2]]
    
    datosAgosto = {}

    for res in resAgosto:
        datosAgosto[res[0]] = [res[1],res[2]]
    
    return datosJulio,datosAgosto

def generarArchivo(julio,agosto):

    archivo = open(SALIDA,'w')
    
    for legajo in julio.keys():
        if(legajo not in agosto):
            print legajo,' not in agosto'
            continue
        
        escala = julio[legajo][0] + agosto[legajo][0]
        ganancia = julio[legajo][1] + agosto[legajo][1]

        archivo.write(QUERY.format(**{
            'legajo':legajo,
            'escala':escala,
            'ganancia':ganancia
        }))

    
    archivo.close()



HOST = '10.10.10.1'#'10.10.10.1'
US = 'owner_rafam'
PASS = 'ownerdba'
SID = 'MSV'#'MSV'


def conectar():
    con = ORA.connect(""+US + "/" + PASS + "@" + HOST + "/" + SID)
    cur = con.cursor()
    return con,cur



def main():

    julio,agosto = obtenerDatos()
    generarArchivo(julio,agosto)

main()