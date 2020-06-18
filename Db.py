import psycopg2
import logging 

logger = logging.getLogger(__name__)

class Postgres:
    def __init__(self, connstr):
        self.conn = psycopg2.connect(connstr)
    
    def insertEmpresa(self, fii):
        logger.info('Inserting  {0}'.format(fii['papel']))
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO EMPRESA (
                    codigoexec,
                    url,
                    papel,
                    nomecomercial,
                    razaosocial,
                    empresa,
                    setor,
                    subsetor,
                    cotacao,
                    dtultimacotacao,
                    tipo,
                    dtultimobalanco,
                    valordemercado,
                    valordefirma,
                    nroacoes,
                    pl,
                    pvp,
                    pebit,
                    psr,
                    pativos,
                    pcg,
                    pacliq,
                    dy,
                    evebitda,
                    evebit,
                    lpa,
                    vpa,
                    margbruta,
                    margebit,
                    margliq,
                    ebitativo,
                    roic,
                    roe,
                    liqcorr,
                    divbrutapat,
                    giroativos,
                    cresrec,
                    ativo,
                    divbruta,
                    disponibilidades,
                    divliq,
                    ativocirculante,
                    patrimonioliq,
                    receitaliq,
                    ebit,
                    lucroliq
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                ) RETURNING id; """, (
                fii['codigoexec'],
                fii['url'],
                fii['papel'],
                fii['nomecomercial'],
                fii['razaosocial'],
                fii['empresa'],
                fii['setor'],
                fii['subsetor'],
                fii['cotacao'],
                fii['dtultimacotacao'],
                fii['tipo'],
                fii['dtultimobalanco'],
                fii['valordemercado'],
                fii['valordefirma'],
                fii['nroacoes'],
                fii['pl'],
                fii['pvp'],
                fii['pebit'],
                fii['psr'],
                fii['pativos'],
                fii['pcg'],
                fii['pacliq'],
                fii['dy'],
                fii['evebitda'],
                fii['evebit'],
                fii['lpa'],
                fii['vpa'],
                fii['margbruta'],
                fii['margebit'],
                fii['margliq'],
                fii['ebitativo'],
                fii['roic'],
                fii['roe'],
                fii['liqcorr'],
                fii['divbrutapat'],
                fii['giroativos'],
                fii['cresrec'],
                fii['ativo'],
                fii['divbruta'],
                fii['disponibilidades'],
                fii['divliq'],
                fii['ativocirculante'],
                fii['patrimonioliq'],
                fii['receitaliq'],
                fii['ebit'],
                fii['lucroliq']
            ))
            r = cur.fetchone()[0]
            self.conn.commit()
            return r

    def insertError(self, error):
        logger.info('Inserting Error for {0}'.format(error['papel']))
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ERRORS (
                    codigoexec,
                    papel,
                    msg
                ) VALUES (%s, %s, %s);""", (
                error['codigoexec'],
                error['papel'],
                error['error']
            ))
            self.conn.commit()

    def insertRendimento(self, fiiid, r):
        logger.info('Inserting Rendimento for id {0}'.format(fiiid))
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO RENDIMENTO (
                    fii,
                    dtpagamento,
                    cotacao,
                    rendimento
                ) VALUES (%s, %s, %s, %s);""", (
                fiiid,
                r.dtpagamento,
                r.cotacao,
                r.rendimento
            ))
            self.conn.commit()

    def getErrors(self):
        logger.info('Getting Errors')
        with self.conn.cursor() as cur:
            cur.execute("""
                select msg, count(id), string_agg(codigo, ' ') 
                from 
                    errors 
                where 
                    codigoexec = (select max(codigoexec) from fii) 
                group by 
                    msg 
                order by 
                    msg;""")
            return cur.fetchall()

    def getTipos(self):
        logger.info('getting Types')
        with self.conn.cursor() as cur:
            cur.execute("""select tipo, count(id) as c 
                from 
                    fii 
                where 
                    codigoexec = (select max(codigoexec) from fii) 
                group by 
                    tipo 
                order by 
                    c desc;""")
            return cur.fetchall()

    def getSubtipos(self):
        logger.info('Getting Subtypes')
        with self.conn.cursor() as cur:
            cur.execute("""select 
	                subtipo, 
                    count(id) as c, 
                    round(avg(dy)::numeric, 2), 
                    round(avg(dya)::numeric, 2) as apvpa,
                    round(avg(pvpa)::numeric,2)
                from 
 	                fii 
                left join (
	                select 
		                fii as fiiid, sum(r.rendimento/f.cotacao)*100 as dya
	                from 
		                fii as f, rendimento as r 
	                where 
		                f.id = r.fii 
	                group by 
		                r.fii 
                ) t2 on fii.id = fiiid
                where 
	                codigoexec = (select max(codigoexec)  from fii) 
                group by 
	                tipo,subtipo 
                order by 
	                c desc;""")
            return cur.fetchall()

    def getRecomendacoes(self):
        logger.info('Getting Recomendations')
        with self.conn.cursor() as cur:
            cur.execute("""select 
                    codigo, tipo, subtipo, 
                    round(dy::numeric, 2), 
                    round(dya::numeric, 2),
                    round(pvpa::numeric, 2), 
                    url, 
                    notas
                from 
                    fii 
                left join (
	                select 
                        fii as fiiid, 
                        sum(r.rendimento/f.cotacao)*100 as dya, 
                        max(dtpagamento) as mdtpgto, 
                        (max(rendimento) - avg(rendimento)) / avg(rendimento) as Ma,
                        (avg(rendimento) - min(rendimento)) / avg(rendimento) as Mi
                        from 
                            fii as f, rendimento as r 
	                    where 
                            f.id = r.fii 
                        group by 
                            r.fii 
                ) t2 on fii.id = fiiid
                where
                    codigoexec = (select max(codigoexec) from fii) and
                    tipo <> 'papel' and 
                    subtipo <> 'desenvolvimento' and
                    mdtpgto > now() - interval '10 months' and 
                    dya >= 7 and 
                    Ma < 0.35 and
                    Mi < 0.35
                order by 
                    pvpa,dy desc""")
            return cur.fetchall()
