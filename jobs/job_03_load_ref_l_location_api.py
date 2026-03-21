# jobs/job_03_load_ref_l_location_api.py - Version finale corrigée
import os
import sys
import uuid
from typing import Optional, List, Dict, Any

# Ajoute scripts au path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
scripts_path = os.path.join(project_root, "scripts")
sys.path.insert(0, scripts_path)

# Charge .env
from dotenv import load_dotenv
load_dotenv(os.path.join(project_root, '.env'))

from odoo_client_odoorpc_fixed import OdooClient
import psycopg2
from psycopg2.extras import execute_values


def m2o_id(v) -> Optional[int]:
    """Extrait l'ID d'un champ Many2one Odoo"""
    return int(v[0]) if isinstance(v, (list, tuple)) and v else None


def norm_site(complete_name: str) -> str:
    """Normalise le nom du site"""
    s = (complete_name or "").upper()
    if s.startswith("DEPOT/CENTRAL"):
        return "DEPOT_CENTRAL"
    if s.startswith("BOUTIQUE"):
        return "BOUTIQUE"
    return "OTHER"


def search_read_all(odoo: OdooClient, model: str, domain: list, fields: list,
                    order: str = "id asc", batch_size: int = 2000) -> List[Dict[str, Any]]:
    """Lecture paginée depuis Odoo"""
    out = []
    offset = 0
    
    print(f"Récupération des {model}...")
    
    while True:
        batch = odoo.execute(
            model, 
            "search_read",
            domain,
            fields=fields,
            offset=offset,
            limit=batch_size,
            order=order
        )
        
        if not batch:
            break
            
        out.extend(batch)
        print(f"  Lot {offset//batch_size + 1}: {len(batch)} enregistrements")
        
        if len(batch) < batch_size:
            break
            
        offset += batch_size
    
    print(f"✓ Total récupéré: {len(out)} enregistrements")
    return out


def main():
    print("=" * 60)
    print("📦 JOB 03: Import des locations Odoo vers PostgreSQL")
    print("=" * 60)
    
    # Vérification des variables
    required_vars = ["ODOO_URL", "ODOO_DB", "ODOO_USER", "ODOO_API_KEY", "DWH_DSN"]
    for var in required_vars:
        if not os.getenv(var):
            raise ValueError(f"Variable {var} non définie dans .env")
    
    dwh_dsn = os.environ["DWH_DSN"]
    
    # 1. Connexion Odoo
    print("\n1. 🔗 Connexion à Odoo Online...")
    odoo = OdooClient()
    odoo.connect()
    
    # 2. Récupération des données
    print("\n2. 📥 Récupération des locations depuis Odoo...")
    fields = ["id", "name", "complete_name", "usage", "active", "location_id"]
    domain = []  # Toutes les locations
    
    locations = search_read_all(odoo, "stock.location", domain, fields, batch_size=50)
    
    # 3. Transformation des données
    print("\n3. 🔄 Transformation des données...")
    rows = []
    for loc in locations:
        loc_id = int(loc["id"])
        complete_name = (loc.get("complete_name") or loc.get("name") or "").strip()
        usage = (loc.get("usage") or "internal").strip()
        parent_id = m2o_id(loc.get("location_id"))

        rows.append((
            str(uuid.uuid4()),                      # l_location_id (UUID)
            loc_id,                                 # l_location_id_odoo
            complete_name,                          # l_complete_name
            usage,                                  # l_usage
            norm_site(complete_name),               # l_site_norm
            str(parent_id) if parent_id else None,  # parent_location
            bool(loc.get("active", True))           # l_is_current
        ))
    
    print(f"   ✓ {len(rows)} lignes préparées")
    
    # Aperçu des premières lignes
    if rows:
        print(f"\n   Aperçu des 3 premières lignes:")
        for i, row in enumerate(rows[:3], 1):
            print(f"   {i}. OdooID: {row[1]}, Nom: {row[2][:30]}..., Usage: {row[3]}")
    
    # 4. Insertion PostgreSQL
    print(f"\n4. 💾 Insertion dans PostgreSQL...")
    
    upsert_sql = """
        INSERT INTO core.ref_l_location (
            l_location_id,
            l_location_id_odoo,
            l_complete_name,
            l_usage,
            l_site_norm,
            parent_location,
            l_is_current
            -- l_created_at et l_updated_at seront gérés automatiquement
        )
        VALUES %s
        ON CONFLICT (l_location_id_odoo)
        DO UPDATE SET
            l_complete_name  = EXCLUDED.l_complete_name,
            l_usage          = EXCLUDED.l_usage,
            l_site_norm      = EXCLUDED.l_site_norm,
            parent_location  = EXCLUDED.parent_location,
            l_is_current     = EXCLUDED.l_is_current,
            l_updated_at     = CURRENT_TIMESTAMP;
    """
    
    try:
        with psycopg2.connect(dwh_dsn) as conn:
            with conn.cursor() as cur:
                # Exécute l'upsert
                execute_values(cur, upsert_sql, rows, page_size=100)
                
                # Vérifie combien de lignes ont été insérées/mises à jour
                cur.execute("SELECT COUNT(*) FROM core.ref_l_location")
                total_count = cur.fetchone()[0]
                
                # Vérifie les nouvelles lignes
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN l_updated_at > NOW() - INTERVAL '5 minutes' THEN 1 ELSE 0 END) as recentes
                    FROM core.ref_l_location
                """)
                stats = cur.fetchone()
                
            conn.commit()
        
        print(f"\n✅ SUCCÈS!")
        print(f"   {len(rows)} lignes traitées")
        print(f"   Total dans la table: {total_count} locations")
        print(f"   Dont {stats[1]} mises à jour récemment")
        print(f"   Table: core.ref_l_location")
        
    except psycopg2.Error as e:
        print(f"\n❌ Erreur PostgreSQL: {e}")
        print(f"\n🔧 Dépannage:")
        print(f"   1. Vérifiez la connexion: psql -h localhost -U blissydah -d blissydah")
        print(f"   2. Vérifiez la table: \\d core.ref_l_location")
        print(f"   3. Vérifiez les permissions")
        raise
        
    except Exception as e:
        print(f"\n❌ Erreur inattendue: {type(e).__name__}: {e}")
        raise
    
    print("\n" + "=" * 60)
    print("🎉 Job terminé avec succès!")
    print("=" * 60)


if __name__ == "__main__":
    main()
