from datetime import date, datetime
import sqlite3
from time import time
import datetime as d


def inserir_repres(codrepres, tipopess, nomefan,comissaobase):
    sql = """INSERT INTO TBL_REPRES 
            (codrepres, tipopess, nomefan, comissaobase)
            VALUES (?, ?, ?, ?)"""
    cursor.execute(sql, (int(codrepres), tipopess, nomefan, float(comissaobase)))

def inserir_produtos(codprod, nomeprod, codforne,unidade, aliqicms, valcusto, valvenda, qtdemin, qtdeestq, grupo, classestq, comissao, pesobruto):
    sql = """INSERT INTO TBL_PRODUTOS 
            (CODPROD, NOMEPROD, CODFORNE, UNIDADE, ALIQICMS, VALCUSTO, VALVENDA, QTDEMIN, QTDEESTQ, GRUPO, CLASSESTQ, COMISSAO, PESOBRUTO) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql, (int(codprod), nomeprod, int(codforne), int(unidade), int(aliqicms), float(valcusto), float(valvenda), float(qtdemin), float(qtdeestq), int(grupo), classestq, float(comissao), float(pesobruto)))

def inserir_fornecedor(codclifor, tipocf, codrepres, nomefan, cidade, uf, codmunicipio, tipopessoa, cobrbanc, prazopgto):
    sql = """INSERT INTO TBL_FORN_CLIEN 
            (CODCLIFOR, TIPOCF, CODREPRES, NOMEFAN, CIDADE, UF, CODMUNICIPIO, TIPOPESSOA, COBRBANC, PRAZOPGTO) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql, (int(codclifor), int(tipocf), codrepres, nomefan, cidade, uf, codmunicipio, tipopessoa, cobrbanc, prazopgto))

def inserir_pedidos(numped, dataped, horaped,codclien, es, finalidnfe, situacao, peso, prazopgto, valorprods, valordesc, valor, valbaseicms, valicms, comissao):
    sql = """INSERT INTO TBL_PEDIDOS 
            (NUMPED, DATAPED, HORAPED, CODCLIEN, ES, FINALIDNFE, SITUACAO, PESO, PRAZOPGTO, VALORPRODS, VALORDESC, VALOR, VALBASEICMS, VALICMS, COMISSAO) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql, (int(numped), dataped, horaped, int(codclien), es, int(finalidnfe), int(situacao), float(peso), int(prazopgto), float(valorprods), float(valordesc), float(valor), float(valbaseicms),float(valicms),float(comissao)))

def inserir_pedidos_item(numped, numitem, codprod, qtde, valunit, unid, aliqicms, comissao, sticms, cfop, reducbaseicms):
    sql = """INSERT INTO TBL_PEDIDOS_ITEM 
            (NUMPED, NUMITEM, CODPROD, QTDE, VALUNIT, UNID, ALIQICMS, COMISSAO, STICMS, CFOP, REDUCBASEICMS) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.execute(sql, (int(numped), int(numitem), int(codprod), float(qtde), float(valunit), unid, float(aliqicms), float(comissao),int(sticms), int(cfop), float(reducbaseicms)))



# Nome do arquivo de entrada
repres_csv = "REPRES.csv"
produtos_csv = "NOVO_PRODUTOS.csv"
fornClien_csv = "FORN_CLIEN.csv"
pedidos_csv = "PEDIDOS.csv"
pedidosItem_csv = "PEDIDOS_ITEM.csv"

conector = sqlite3.connect("DadosExcel.db")
cursor = conector.cursor()

# Criando tabela Repres
sqlRepres = """CREATE TABLE TBL_REPRES (
                    CODREPRES INTEGER PRIMARY KEY, 
                    TIPOPESS TEXT,
                    NOMEFAN TEXT,
                    COMISSAOBASE REAL)"""
cursor.execute(sqlRepres)

#Criando tabela Produtos
#mudanças:
#   aliq int
#   qtdmin int
sqlProdutos = """CREATE TABLE TBL_PRODUTOS (
                    CODPROD INTEGER PRIMARY KEY,
                    NOMEPROD TEXT,
                    CODFORNE INTEGER,
                    UNIDADE INTEGER,
                    ALIQICMS FLOAT, 
                    VALCUSTO REAL,
                    VALVENDA REAL,
                    QTDEMIN REAL,
                    QTDEESTQ REAL,
                    GRUPO INTEGER,
                    CLASSESTQ TEXT,
                    COMISSAO INTEGER,
                    PESOBRUTO REAL)"""
cursor.execute(sqlProdutos)

#Criando tabela FornClien
# mudanças:
#   cobrbanc int
sqlFornecedor = """CREATE TABLE TBL_FORN_CLIEN (
                    CODCLIFOR INTEGER PRIMARY KEY,
                    TIPOCF INTEGER,
                    CODREPRES INTEGER,
                    NOMEFAN TEXT,
                    CIDADE TEXT,
                    UF TEXT,
                    CODMUNICIPIO INTEGER,
                    TIPOPESSOA INTEGER,
                    COBRBANC INTEGER,
                    PRAZOPGTO INTEGER)"""
cursor.execute(sqlFornecedor)

#Criando tabela Pedidos
sqlPedidos = """CREATE TABLE TBL_PEDIDOS (
                    NUMPED INTEGER PRIMARY KEY,
                    DATAPED DATE,
                    HORAPED TIME,
                    CODCLIEN INTEGER,
                    ES TEXT,
                    FINALIDNFE INTEGER,
                    SITUACAO INTEGER,
                    PESO REAL,
                    PRAZOPGTO INTEGER,
                    VALORPRODS REAL,
                    VALORDESC REAL,
                    VALOR REAL,
                    VALBASEICMS REAL,
                    VALICMS REAL,
                    COMISSAO REAL)"""
cursor.execute(sqlPedidos)

#mudanças:
    # aliq integer
    # comissao integer
sqlItem = """CREATE TABLE TBL_PEDIDOS_ITEM (
                ID_PEDIDO INTEGER PRIMARY KEY AUTOINCREMENT,
                NUMPED INTEGER,
                NUMITEM INTEGER,
                CODPROD INTEGER,
                QTDE REAL,
                VALUNIT REAL,
                UNID TEXT,
                ALIQICMS REAL,
                COMISSAO INTEGER,
                STICMS INTEGER,
                CFOP INTEGER,
                REDUCBASEICMS REAL)"""
cursor.execute(sqlItem)

conector.commit()

print("Banco criado com sucesso!")


linha1 = True
    #Inserindo dados na TBL_REPRES
with open(repres_csv, "r", encoding="utf-8") as arquivo1:
    cont_vendas = 0
    
    for linha in arquivo1:
        if linha1:
            linha1 = False
            continue
        
        cont_vendas += 1
        print(linha)
        info = linha.strip().split(";")

        #if info[0].isdigit:
        if info[0].strip():
            codrepres = int(info[0])
        else:
            codrepres = -99
        tipopess = info[1]
        nomefan = info[2]
        comissaobase = float(info[3])

        inserir_repres(codrepres, tipopess, nomefan, comissaobase)
    conector.commit()

linha1 = True
with open(produtos_csv, "r", encoding="utf-8") as arquivo2:
    cont_vendas = 0

    for linha in arquivo2:
        if linha1:
            linha1 = False
            continue

        cont_vendas += 1
        print(linha)
        info = linha.strip().split(";")

        codprod = int(info[0])
        nomeprod = info[1]
        if info[2].strip():
            codforne = int(info[2])
        else:
            codforne = -99
        unidade = int(info[3])
        #aliqicms = float(info[4].replace(',', '.'))
        if info[4].strip():
            aliqicms = float(info[4])
        else:
            aliqicms = -99
        if info[5].strip():
            valcusto = float(info[5])
        else:
            valcusto = -99
        if info[6].strip():
            valvenda = float(info[6])
        else:
            valvenda -99
        if info[7].strip():
            qtdemin = float(info[7])
        else:
            qtdemin = -99
        if info[8].strip():
            qtdeestq = float(info[8])
        else:
            qtdeestq = -99
        grupo = int(info[9])
        classestq = info[10]
        if info[11].strip():
            comissao = int(info[11])
        else:
            comissao = -99
        if info[12].strip():
            pesobruto = float(info[12])
        else:
            pesobruto = -99

        inserir_produtos(codprod, nomeprod, codforne, unidade, aliqicms, valcusto, valvenda, qtdemin, qtdeestq, grupo, classestq, comissao,pesobruto)
    conector.commit()

linha1 = True
with open(fornClien_csv, "r", encoding="utf-8") as arquivo3:
    cont_vendas = 0

    for linha in arquivo3:
        if linha1:
            linha1 = False
            continue

        cont_vendas += 1
        info = linha.strip().split(";")

        if info[0].strip():
            codclifor = int(info[0])
        else:
            codclifor = -99
        if info[1].strip():
            tipocf = int(info[1])
        else:
            tipocf = -99
        if info[2].strip():
            codrepres = int(info[2])
        else:
            codrepres = -99
        nomefan = info[3]
        cidade = info[4]
        uf = info[5]
        if info[6].strip():
            codmunicipio = int(info[6])
        else:
            codmunicipio = -99
        if info[7].strip():
            tipopessoa = int(info[7])
        else:
            tipopessoa = -99
        if info[7].strip():
            cobrbanc = int(info[8])
        else:
            cobrbanc = -99
        if info[9].strip():
            prazopgto = int(info[9])
        else:
            prazopgto = -99

        inserir_fornecedor(codclifor, tipocf, codrepres, nomefan, cidade, uf, codmunicipio, tipopessoa, cobrbanc, prazopgto)
    conector.commit()

linha1 = True
with open(pedidos_csv, "r", encoding="utf-8") as arquivo4:
    cont_vendas = 0

    for linha in arquivo4:
        if linha1:
            linha1 = False
            continue

        cont_vendas += 1
        info = linha.strip().split(";")

        # if info[0].isdigit:
        numped = int(info[0])
        dataped = d.datetime.strptime(info[1], "%d/%m/%Y").strftime("%x")
        horaped = d.datetime.strptime(info[2], "%H:%M:%S").strftime("%X")

        if info[3].strip():
            codclien = int(info[3])
        else:
            codclien = -99
        es = info[4]
        finalidnfe = int(info[5])
        situacao = int(info[6])
        peso = float(info[7])
        if info[8].strip():
            prazopgto = int(info[8])
        else:
            prazopgto = -99   
        valorprods = float(info[9])
        valordesc = float(info[10])
        valor = float(info[11])
        valbaseicms = float(info[12])
        valicms = float(info[13])
        comissao = float(info[14])

        # Se todas as variáveis foram convertidas corretamente, insira os dados
        inserir_pedidos(numped, dataped, horaped, codclien, es, finalidnfe, situacao, peso, prazopgto, valorprods, valordesc, valor, valbaseicms, valicms, comissao)
    conector.commit()

linha1 = True
with open(pedidosItem_csv, "r") as arquivo5:
    cont_vendas = 0

    for linha in arquivo5:
        if linha1:
            linha1 = False
            continue
        cont_vendas += 1
        print(linha)
        info = linha.strip().split(";")

        numped = int(info[0])
        numitem = int(info[1])
        codprod = int(info[2])
        qtde = float(info[3])
        valunit = float(info[4])
        unid = info[5]
        if info[6].strip():
            aliqicms = float(info[6])
        else:
            aliqicms -99
        if info[7].strip():
            comissao = float(info[7])
        else:
            comissao -99
        sticms = int(info[8])
        if info[9].strip():
            cfop = int(info[9])
        else:
            cfop -99
        reducbaseicms = float(info[10])
            
        inserir_pedidos_item(numped, numitem, codprod, qtde, valunit, unid, aliqicms, comissao, sticms, cfop, reducbaseicms)
    conector.commit()

cursor.close()
conector.close()


print("\nFim do programa")
