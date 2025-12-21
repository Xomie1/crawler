"""
Simple AI verification script
Tests the hybrid crawler and outputs clean JSON results
"""

import json
from crawler.engine import CrawlerEngine

def main():
    """Run crawl and display JSON results."""
    
    # Test URLs
    test_urls = [
        # "http://www.ping-pongs.jp/cor.htm"
        # "https://31shobo.com/%E4%BC%9A%E7%A4%BE%E6%A6%82%E8%A6%81/",
        # "http://isr-office.net/prof.html"
        # "http://okinawa-ladies.jp"
        # "https://andperson.com/"
        # "http://data-search.co.jp"
        # "https://www.castingdoctor.jp/company/"
        # "http://www.tantei-boggy.com/company/company.html#detail"
        # "http://www.osero.net/kaisya.html"
        #  "http://gyouseishosi.gifu.jp/office"
        # "https://www.sij-investigation.com/弊所概要",
        # "https://www.sakuradamon.net/gaiyou.html#gaiyou"
        # "https://finesse-site.com/about/"
        # "http://xn--1lqs71d2law9k8zbv08f.tokyo/company"
        # "https://galu-matsudo.com/company/"
        # "http://r-walker.jp/company.html"
        # "https://oncode-inc.com/company.html#profile",
        # "https://www.nippon-tk.jp/company/",
        # "https://achievement.jp/company.php",
        # "https://aoi-ueno.co.jp/company/",
        # "https://galu-daiba.com/company/",
        # "https://smile-agent-jp.com/companyoutline/",
        # "https://nextdoor-det.com/transaction.html",
        # "https://tochi-tan.com/about/",
        # "https://www.redif.co.jp/会社概要/",
        # "http://maisonlibre.tokyo/#about",
        # "http://hiraihitomi.com",
        # "https://www.minji-chosa.jp/aboutus/office/",
        # "http://www.gyousei-n.jp/guide-office",
        # "https://www.tantei-mr.co.jp/about/",
        # "https://www.rising4.com/office/",
        # "http://www.ability-office.co.jp/profile/index.html",
        # "http://www.plot-corp.jp/",
        # "http://www.iris-staff.com/kaisyaannai.html",
        # "http://tda-web.com/outline.html",
        # "http://www.yokoyama-design.com/company.html",
        # "https://www.hatsukoi.co.jp/outline/#anc02",
        # "http://fujimaru.org/company/#gaiyou",
        # "https://human24.jp/company/",
        # "http://k-chousa.com/info.html",
        # "https://eagle-eye.co.jp/company",
        # "https://troffice.jp/about/",
        # "https://www.libra-system.com/",
        # "http://www.shimatomo.com/company.php?afkey=",
        # "https://www.akai-tantei.com/share/cor.htm",
        # "https://1-call.co.jp/%e4%bc%9a%e7%a4%be%e6%a6%82%e8%a6%81",
        # "http://www.starzway.com/company.html",
        # "https://www.chu-ou.com/gaiyo.php",
        # "https://www.ncia-research.or.jp/tokucho/",
        # "http://daichokyo.or.jp/",
        # "https://mirail-inc.com/company/",
        # "http://www.worldoffice.co.jp/corporate.html",
        # "http://www.hikari-partners.co.jp/corporate_profile.html",
        # "https://hytokyo.co.jp/about/",
        # "https://www.jda-tokyo.jp/category/1470885.html",
        # "https://book-arrows.jp/%E9%81%8B%E5%96%B6%E4%BC%9A%E7%A4%BE/",
        # "https://www.toyama-law-office.jp/company/",
        # "https://www.ks110.com/prof/index.html",
        # "https://peress.jp/about-us/company/",
        # "https://www.coco-service.co.jp/company",
        # "https://www.credix-web.co.jp/company/index.html",
        # "https://www.alpha-note.co.jp/about",
        # "https://masuku.jp/about/index.html",
        # "https://gk-utsunomiya.com/top-2/company/",
        # "https://u-style-saho.com/company",
        # "https://www.kokoro110.jp/profile",
        # "https://www.tr-office.jp/about/",
        # "https://www.tantei.co.jp/company",
        # "https://fujiresearch.jp/about/",
        # "https://life-renovation.com/company/",
        # "http://www.national-agent.co.jp/company.html",
        # "https://www.kishimoto-sr.co.jp/company/",
        # "http://www.a-agent.net/company/",
        # "https://rikon-tantei.com/",
        # "http://hayabusa-tantei.com/",
        # "https://www.lass.ne.jp/contents4-1",
        # "https://www.aichi-ac.co.jp/company/",
        # "http://o-plus511.com/",
        # "http://www.tcgr.jp/",
        # "https://business-link-service.com/9-access.html",
        # "http://www.nozomi-tantei.com/company.html",
        # "https://www.report-d.com/profile.html",
        # "http://www.s-tantei.com/gaiyou.html",
        # "https://ihara-d.com/aboutus/",
        # "https://www.n-tantei.com/company/",
        # "http://www.tanteisan.com/",
        # "http://www.world-research.jp/company/",
        # "http://www.cardkessai-online.jp/会社案内.html",
        # "https://www.yts24.com/campany/",
        # "https://roppongi-tantei.com/company",
        # "https://detect.jp/pages/office.html",
        # "http://www.cia-7.jp/company.html",
        # "https://result-tokyo.com/corporate_profile/",
        # "https://iceblue.jp/ns/category3/entry12.html",
        # "https://tantei365.com/company",
        # "http://acala.net/",
        # "https://sendaikeyaki.com/profile/index.html",
        # "https://www.chosakai.com/kaisyagaiyou.htm",
        # "http://www.i-probe.net/new.kaisya.html",
         "http://www.of-ys.com/outline.html",
        # "https://aizu-tantei.jp/about.html",
        # "http://www.sumidagawa-tantei.jp/contact.html#company",
        # "https://www.uwakityosa.com/会社案内",
        # "https://www.tantei-anju.com/kaisya.html",
        # "http://jt-r.com/kaishagaiyou.htm",
        # "https://www.hri-hama.co.jp/about/"

        
    ]
    
    results = []
    
    for url in test_urls:
        try:
            print(f"Crawling: {url}")
            
            crawler = CrawlerEngine(
                root_url=url,
                use_ai_extraction=True,
                ai_provider='groq',
                use_enhanced_form_detection=True
            )
            
            result = crawler.crawl()
            crawler.close()
            
            # Format clean JSON output
            clean_result = {
                "url": result.get('url'),
                "email": result.get('email'),
                "inquiryFormUrl": result.get('inquiryFormUrl'),
                "companyName": result.get('companyName'),
                "industry": result.get('industry'),
                "httpStatus": result.get('httpStatus'),
                "robotsAllowed": result.get('robotsAllowed'),
                "crawlStatus": result.get('crawlStatus'),
                "errorMessage": result.get('errorMessage')
            }
            
            results.append(clean_result)
            print(json.dumps(clean_result, ensure_ascii=False, indent=2))
            print()
            
        except Exception as e:
            print(f"Error: {e}\n")

if __name__ == "__main__":
    main()