from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import NoSuchElementException
import logging
from datetime import datetime
from unicodedata import normalize

logger = logging.getLogger(__name__)

class Crowler:
    def __init__(self, timeout=45):
        op = webdriver.ChromeOptions()
        op.add_argument('headless')
        op.add_argument('--no-sandbox')
        caps = DesiredCapabilities().CHROME
        caps["pageLoadStrategy"] = "eager"
        self.driver = webdriver.Chrome(options=op, desired_capabilities=caps)
        self.driver.set_page_load_timeout(timeout)
        self.driver.implicitly_wait(1)

    def __del__(self):
        try:
            self.driver.close()
        except Exception as error:
            logger.error('Error closing driver: {0}'.format(str(error)))

    def getAll(self):
        logger.info('Getting Enterprises')
        try:
            self.driver.get('http://www.fundamentus.com.br/detalhes.php')
            t = self.driver.find_element_by_id('test1')
            tbody = t.find_element_by_tag_name('tbody')
            return [
                [c.text for c in row.find_elements_by_tag_name('td')]
                for row in tbody.find_elements_by_tag_name('tr')
            ]
        except Exception as error:
            logger.error('Error getting enterprises: {0}'.format(str(error)))
            return []

    def toInt(self, n):
        if not isinstance(n, str): return None
        return int(n.replace('.', ''))

    def toFloat(self, n):
        if not isinstance(n, str): return None
        i = n.replace(' ', '').replace('.', '').replace(',', '.').replace('%', '').replace('R$', '')
        if 'B' in i: return float(i.replace('B', ''))*1000000000
        if 'M' in i: return float(i.replace('M', ''))*1000000
        if '-' == i: return None
        return float(i)

    def getLastRevenues(self):
        logger.info('getLastRevenues')
        bts = self.driver.find_elements_by_class_name('dh')
        href = next((bt.get_attribute('href') for bt in bts if 'proventos' in bt.get_attribute('href')), None)
        if href is None:
            raise Exception('Button required for proventos not found')
        logger.info('Getting Proventos from {0}'.format(href))
        self.driver.get(href)
        revenues = []
        try:
            t = self.driver.find_element_by_id('resultado')
            tbody = t.find_element_by_tag_name('tbody')
        except NoSuchElementException:
            c = self.driver.find_element_by_class_name('conteudo')
            if 'nenhum provento encontrado' in c.text.lower():
                return revenues
            else:
                raise Exception('Table "resultado" required for proventos not found')
        for row in tbody.find_elements_by_tag_name('tr'):
            colums = [c.text for c in row.find_elements_by_tag_name('td')]
            revenues.append({
                'dtpgto': self.toDate(colums[0]),
                'valor': self.toFloat(colums[1]),
                'tipo': colums[2].replace(' ', '').lower(),
                'qntacoes': self.toInt(colums[3])
            })
        return revenues

    def nK(self, s):
        s = s.replace(' ', '').lower()
        return normalize('NFKD', s).encode('ASCII', 'ignore').decode('ASCII')

    def toDate(self, d):
        if not isinstance(d, str): return None
        return datetime.strptime(d, '%d/%m/%Y').date()

    def clean(self, ofii):
        logger.debug('clean')
        fii = {self.nK(k): v for k,v in ofii.items()}
        fii['papel'] = fii['papel'].lower()
        logger.debug(str(fii))
        fii['cotacao'] = self.toFloat(fii.get('cotacao'))
        fii['dtultimacotacao'] = self.toDate(fii.get('dataultcot'))
        fii['dtultimobalanco'] = self.toDate(fii.get('ultbalancoprocessado'))
        fii['valordemercado'] = self.toFloat(fii.get('valordemercado'))
        fii['valordefirma'] = self.toFloat(fii.get('valordafirma'))
        fii['nroacoes'] = self.toInt(fii.get('nro.acoes'))
        fii['pl'] = self.toFloat(fii.get('p/l'))
        fii['pvp'] = self.toFloat(fii.get('p/vp'))
        fii['pebit'] = self.toFloat(fii.get('p/ebit'))
        fii['psr'] = self.toFloat(fii.get('psr'))
        fii['pativos'] = self.toFloat(fii.get('p/ativos'))
        fii['pcg'] = self.toFloat(fii.get('p/cap.giro'))
        fii['pacliq'] = self.toFloat(fii.get('p/ativcircliq'))
        fii['dy'] = self.toFloat(fii.get('div.yield'))
        fii['evebitda'] = self.toFloat(fii.get('ev/ebitda'))
        fii['evebit'] = self.toFloat(fii.get('ev/ebit'))
        fii['lpa'] = self.toFloat(fii.get('lpa'))
        fii['vpa'] = self.toFloat(fii.get('vpa'))
        fii['margbruta'] = self.toFloat(fii.get('marg.bruta'))
        fii['margebit'] = self.toFloat(fii.get('marg.ebit'))
        fii['margliq'] = self.toFloat(fii.get('marg.liquida'))
        fii['ebitativo'] = self.toFloat(fii.get('ebit/ativo'))
        fii['roic'] = self.toFloat(fii.get('roic'))
        fii['roe'] = self.toFloat(fii.get('roe'))
        fii['liqcorr'] = self.toFloat(fii.get('liquidezcorr'))
        fii['divbrutapat'] = self.toFloat(fii.get('divbr/patrim'))
        fii['giroativos'] = self.toFloat(fii.get('giroativos'))
        fii['cresrec'] = self.toFloat(fii.get('cres.rec(5a)'))
        fii['ativo'] = self.toInt(fii.get('ativo'))
        fii['divbruta'] = self.toInt(fii.get('div.bruta'))
        fii['disponibilidades'] = self.toInt(fii.get('disponibilidades'))
        fii['divliq'] = self.toInt(fii.get('div.liquida'))
        fii['ativocirculante'] = self.toInt(fii.get('ativocirculante'))
        fii['patrimonioliq'] = self.toInt(fii.get('patrim.liq'))
        fii['receitaliq'] = self.toInt(fii.get('receitaliquida'))
        fii['ebit'] = self.toInt(fii.get('ebit'))
        fii['lucroliq'] = self.toInt(fii.get('lucroliquido'))
        fii['cartdecredito'] = self.toInt(fii.get('cart.decredito'))
        fii['depositos'] = self.toInt(fii.get('depositos'))
        fii['liquidezcorr'] = self.toInt(fii.get('liquidezcorr'))
        fii['recservicos'] = self.toInt(fii.get('recservicos'))
        fii['resultintfinanc'] = self.toInt(fii.get('resultintfinanc'))
        return fii

    def getDetail(self, papel):
        logger.info('Getting Details for {0}'.format(papel))
        try:
            url = 'http://www.fundamentus.com.br/detalhes.php?papel={0}'.format(papel)
            self.driver.get(url)
            atts = {
                'url': url,
                'papel': papel
            }
            tables = self.driver.find_elements_by_class_name('w728')
            if not tables:
                raise Exception('No info found')
            for t in tables:
                k = None
                for row in t.find_elements_by_tag_name('td'):
                    if row.get_attribute('class').find('label') != -1:
                        k = row.find_element_by_class_name('txt').text
                    if k and row.get_attribute('class').find('data') != -1:
                        atts[k] = row.text
                        k = None
            atts['proventos'] = self.getLastRevenues()
            return self.clean(atts)
        except Exception as error:
            logger.error('Error getting detail: {0}'.format(str(error)))
            return {'papel': papel, 'error': str(error)}

