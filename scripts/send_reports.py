import os
import smtplib
import logging
import glob
from email.message import EmailMessage
from datetime import datetime
from dotenv import load_dotenv

# --- CONFIGURATION ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))
load_dotenv(os.path.join(PROJECT_ROOT, "config", "db.env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("MailService")

def send_combined_reports():
        # Paramètres serveur Blissydah
    SMTP_SERVER = "mail.blissydah.com"
    SMTP_PORT = 465
    EMAIL_USER = "norbert.wupona@blissydah.com"
    EMAIL_PASS = os.getenv("EMAIL_PASSWORD")
    EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER", "norbert.wupona@blissydah.com")

    OUTPUT_DIR = os.path.join(PROJECT_ROOT, "reports", "outputs")

    msg = EmailMessage()
    today_str = datetime.now().strftime('%d/%m/%Y')
    msg['Subject'] = f"📊 Rapports Quotidiens Blissydah - {today_str}"
    msg['From'] = EMAIL_USER
    msg['To'] = EMAIL_RECEIVER

    # 1. ON INITIALISE LE TEXTE (BODY) D'ABORD
    body_text = f"Bonjour,\n\nVeuillez trouver ci-joint les derniers rapports d'activité Blissydah :\n\n"
    
    report_keywords = ["Rapport_Encaissement", "Stock_Opening", "Stock_Exceptions", "Situation_Stock", "Vente_Detaillee"]
    attachments_to_add = [] # Liste temporaire pour stocker les infos des fichiers trouvés

    for kw in report_keywords:
        search_pattern = os.path.join(OUTPUT_DIR, f"*{kw}*.xlsx")
        files = glob.glob(search_pattern)
        
        if files:
            latest_file = max(files, key=os.path.getmtime)
            file_name = os.path.basename(latest_file)
            mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            date_status = " (Fichier récent)" if mtime.date() == datetime.now().date() else " (Ancien fichier)"
            
            # On prépare la liste des pièces jointes et on complète le texte
            attachments_to_add.append(latest_file)
            body_text += f"✅ {file_name}{date_status}\n"
        else:
            body_text += f"❌ Aucun rapport trouvé pour : {kw}\n"

    body_text += "\n---\nCeci est un message automatique du serveur Blissydah."
    
    # 2. ON FIXE LE CONTENU DU CORPS DE L'EMAIL
    msg.set_content(body_text)

    # 3. ON AJOUTE LES PIÈCES JOINTES APRÈS LE TEXTE
    for file_path in attachments_to_add:
        file_name = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            file_data = f.read()
            msg.add_attachment(
                file_data,
                maintype='application',
                subtype='vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                filename=file_name
            )

    # 4. ENVOI
    if not attachments_to_add:
        logger.warning("⚠️ Aucun fichier n'a pu être collecté. Envoi annulé.")
        return

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        logger.info(f"🚀 Succès ! {len(attachments_to_add)} rapports envoyés.")
    except Exception as e:
        logger.error(f"💥 Erreur d'envoi : {e}")

if __name__ == "__main__":
    send_combined_reports()
