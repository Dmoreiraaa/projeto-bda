
# -*- coding: utf-8 -*-
"""BDA - JDE | Streamlit

MUDANÇA SOLICITADA (perfis fixos):
- Dois perfis: CONFIABILIDADE e TECNICO
- TECNICO: pode REGISTRAR (criar/INSERT) e pode CONSULTAR (somente leitura na edição)
- CONFIABILIDADE: acesso total (registrar + editar + dashboard)

IMPORTANTE (segurança):
- Para NÃO deixar senha “hardcoded” no arquivo (risco), as senhas são lidas de variáveis de ambiente:
    - SENHA_CONFIABILIDADE
    - SENHA_TECNICO
  Você pode definir os valores desejados (ex.: jdemanutencao / 123456) no ambiente ao iniciar.

Auditoria:
- Registra quem criou/editou em bda.criado_por / bda.atualizado_por / bda.atualizado_em.
- Como as credenciais são por perfil compartilhado, o app pede também "Seu e-mail" no login para auditoria.

Requisitos mantidos:
- Aba "Registrar BDA" exclusiva para criação (INSERT)
- Aba "Consulta/Editar" permite abrir BDAs existentes e editar somente se tiver permissão
- Formulário reutilizável: formulario_bda(modo, dados=None, somente_leitura=False)
- PDF (ReportLab) gerado a partir do banco e inclui foto anexada
- SQLite com migração segura (não quebra banco atual)
"""

import os
import json
import sqlite3
from datetime import datetime, date, timedelta, time as dt_time
import uuid
from io import BytesIO
import hmac

import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
from PIL import Image

# PDF (ReportLab)
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle


# ==============================
# JDE Theme
# ==============================
JDE_BROWN = "#3E372D"
JDE_BROWN_MED = "#5A5045"
JDE_CARAMEL = "#C7A97B"
JDE_TERRACOTTA = "#B4552B"
JDE_TEAL = "#1F4E59"
JDE_BG = "#F8F7F3"
JDE_CARD = "#E9E6E1"
TEXT_DARK = "#2B2B2B"
TEXT_LIGHT = "#F8F7F3"

st.set_page_config(
    page_title="BDA - JDE | Formulário e Análise de Quebras",
    layout="wide",
)

alt.themes.register(
    "jde_dark",
    lambda: {
        "config": {
            "view": {"stroke": "transparent"},
            "axis": {"labelColor": JDE_BROWN, "titleColor": JDE_BROWN, "gridColor": "#d7d1c9"},
            "legend": {"labelColor": JDE_BROWN, "titleColor": JDE_BROWN},
            "title": {"color": JDE_BROWN},
        }
    },
)
alt.themes.enable("jde_dark")

css_template = """""".format(
    JDE_BROWN=JDE_BROWN,
    JDE_BROWN_MED=JDE_BROWN_MED,
    JDE_CARAMEL=JDE_CARAMEL,
    JDE_TERRACOTTA=JDE_TERRACOTTA,
    JDE_TEAL=JDE_TEAL,
    JDE_BG=JDE_BG,
    JDE_CARD=JDE_CARD,
    TEXT_DARK=TEXT_DARK,
    TEXT_LIGHT=TEXT_LIGHT,
)
st.markdown(css_template, unsafe_allow_html=True)

DB_PATH = "bda.db"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ==============================
# DB connection
# ==============================
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row
cur = conn.cursor()


# ==============================
# Schema: tabelas e migrações
# ==============================

def ensure_schema():
    # Tabela principal
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bda (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            equipamento TEXT,
            secao TEXT,
            data_quebra DATE,
            hora_quebra TIME,
            tempo_reparo_h REAL,
            numero_ordem TEXT,
            numero_bda TEXT,
            turno TEXT,
            centro_custo TEXT,
            aconteceu_onde TEXT,
            aconteceu_antes TEXT,
            descricao_reparo TEXT,
            modo_falha TEXT,
            acoes_corretivas TEXT,
            responsavel_corretiva TEXT,
            quando_corretiva DATE,
            plano_sap TEXT,
            descricao_plano TEXT,
            responsavel_plano TEXT,
            periodicidade_dias INTEGER,
            ultima_realizacao DATE,
            caminho_imagem TEXT,
            criticidade TEXT,
            categoria TEXT,
            classificacao TEXT,
            causa_raiz TEXT,
            cinco_porques TEXT,
            componentes TEXT,
            custo_pecas REAL,
            custo_mo REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            time_bda TEXT,
            dono_bda TEXT,
            categoria_evento TEXT,
            cinco_porques_grid TEXT,
            acoes_lista TEXT,
            ultimo_executante TEXT,
            status_plano TEXT,
            existe_plano TEXT,
            principio_funcionamento TEXT,
            causas_linhas TEXT
        )
        """
    )
    conn.commit()

    # Migração segura de colunas (inclui auditoria)
    new_cols = {
        "time_bda": "TEXT",
        "dono_bda": "TEXT",
        "categoria_evento": "TEXT",
        "cinco_porques_grid": "TEXT",
        "acoes_lista": "TEXT",
        "ultimo_executante": "TEXT",
        "status_plano": "TEXT",
        "existe_plano": "TEXT",
        "principio_funcionamento": "TEXT",
        "causas_linhas": "TEXT",
        "criado_por": "TEXT",
        "atualizado_por": "TEXT",
        "atualizado_em": "TIMESTAMP",
    }
    cur.execute("PRAGMA table_info(bda)")
    existing = {r[1] for r in cur.fetchall()}
    for col, typ in new_cols.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE bda ADD COLUMN {col} {typ}")
    conn.commit()


ensure_schema()


# ==============================
# Perfis e senhas (via ENV)
# ==============================

def get_role_passwords():
    """Carrega senhas dos perfis via variáveis de ambiente.

    Para atender seu pedido, você pode definir:
      SENHA_CONFIABILIDADE=jdemanutencao
      SENHA_TECNICO=123456

    Obs.: não hardcodamos no arquivo para evitar risco de segurança.
    """
    pw_conf = os.getenv("SENHA_CONFIABILIDADE", "").strip()
    pw_tec = os.getenv("SENHA_TECNICO", "").strip()
    return pw_conf, pw_tec


def role_permissions(role: str) -> dict:
    role = (role or "").upper().strip()
    if role == "CONFIABILIDADE":
        return {"pode_registrar": True, "pode_editar": True, "pode_dashboard": True}
    # TECNICO
    return {"pode_registrar": True, "pode_editar": False, "pode_dashboard": True}


# ==============================
# Utils gerais
# ==============================

def salvar_imagem(upload):
    if upload is None:
        return None
    ext = os.path.splitext(upload.name)[1].lower() or ".png"
    name = f"bda_{uuid.uuid4().hex}{ext}"
    dest = os.path.join(UPLOAD_DIR, name)
    try:
        img = Image.open(upload)
        img.save(dest)
    except Exception:
        with open(dest, "wb") as f:
            f.write(upload.getbuffer())
    return dest


def df_from_query(sql, params=None):
    return pd.read_sql_query(sql, conn, params=params or [])


def all_filled(values: dict, labels: dict) -> list:
    errs = []
    for k, v in values.items():
        if v is None:
            errs.append(f"Preencha o campo: {labels.get(k, k)}")
        elif isinstance(v, str) and not v.strip():
            errs.append(f"Preencha o campo: {labels.get(k, k)}")
    return errs


def _parse_date(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            return pd.to_datetime(s, errors="coerce").date()
        except Exception:
            return None
    return None


def _parse_time(val):
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return datetime.now().time()
    if isinstance(val, dt_time):
        return val
    if isinstance(val, datetime):
        return val.time()
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return datetime.now().time()
        try:
            parts = s.split(":")
            hh = int(parts[0])
            mm = int(parts[1]) if len(parts) > 1 else 0
            ss = int(parts[2]) if len(parts) > 2 else 0
            return dt_time(hour=hh, minute=mm, second=ss)
        except Exception:
            return datetime.now().time()
    return datetime.now().time()


def _safe_json_loads(text, default):
    if text is None:
        return default
    if isinstance(text, (list, dict)):
        return text
    if isinstance(text, str) and not text.strip():
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


def normalizar_dados_bda(dados: dict) -> dict:
    if not dados:
        return {}
    d = dict(dados)
    d["data_quebra"] = _parse_date(d.get("data_quebra")) or date.today()
    d["hora_quebra"] = _parse_time(d.get("hora_quebra"))
    d["ultima_realizacao"] = _parse_date(d.get("ultima_realizacao"))

    try:
        d["tempo_reparo_h"] = float(d.get("tempo_reparo_h") or 0.0)
    except Exception:
        d["tempo_reparo_h"] = 0.0

    try:
        d["periodicidade_dias"] = int(d.get("periodicidade_dias") or 0)
    except Exception:
        d["periodicidade_dias"] = 0

    d["cinco_porques_grid"] = _safe_json_loads(d.get("cinco_porques_grid"), default=[])
    d["acoes_lista"] = _safe_json_loads(d.get("acoes_lista"), default=[])
    d["causas_linhas"] = _safe_json_loads(d.get("causas_linhas"), default=[])

    for k in [
        "equipamento",
        "secao",
        "numero_ordem",
        "numero_bda",
        "turno",
        "time_bda",
        "dono_bda",
        "categoria_evento",
        "componentes",
        "principio_funcionamento",
        "aconteceu_onde",
        "aconteceu_antes",
        "descricao_reparo",
        "modo_falha",
        "plano_sap",
        "descricao_plano",
        "ultimo_executante",
        "status_plano",
        "existe_plano",
        "caminho_imagem",
        "criado_por",
        "atualizado_por",
        "atualizado_em",
    ]:
        if d.get(k) is None:
            d[k] = ""

    if d.get("existe_plano") not in ("Sim", "Não"):
        d["existe_plano"] = "Sim" if d.get("plano_sap") else "Não"

    return d


# ==============================
# Login (2 perfis)
# ==============================

def login_page():
    st.title("BDA – Login")
    st.caption("Selecione o perfil e informe a senha. Para auditoria, informe também seu e-mail.")

    pw_conf, pw_tec = get_role_passwords()
    if not pw_conf or not pw_tec:
        st.error(
            "As variáveis de ambiente SENHA_CONFIABILIDADE e SENHA_TECNICO não estão definidas. "
            "Defina-as e reinicie o app."
        )
        st.code(
            """
Windows (PowerShell):
  $env:SENHA_CONFIABILIDADE='jdemanutencao'
  $env:SENHA_TECNICO='123456'
  streamlit run app.py

Linux/Mac:
  export SENHA_CONFIABILIDADE='jdemanutencao'
  export SENHA_TECNICO='123456'
  streamlit run app.py
""".strip()
        )
        return

    with st.form("login_form", clear_on_submit=False):
        role = st.selectbox("Perfil", ["TECNICO", "CONFIABILIDADE"], index=0)
        user_email = st.text_input("Seu e-mail (para auditoria)", placeholder="nome.sobrenome@empresa.com").strip().lower()
        senha = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar", use_container_width=True)

    if ok:
        if not user_email or "@" not in user_email:
            st.error("Informe um e-mail válido para auditoria.")
            return

        expected = pw_conf if role == "CONFIABILIDADE" else pw_tec
        if not hmac.compare_digest(str(senha), str(expected)):
            st.error("Senha inválida.")
            return

        perms = role_permissions(role)
        st.session_state["auth"] = {
            "role": role,
            "email": user_email,
            **perms,
        }
        st.success("Login realizado!")
        st.rerun()


def require_login():
    if "auth" not in st.session_state or not st.session_state["auth"]:
        login_page()
        st.stop()


def current_user():
    return st.session_state.get("auth", {})


require_login()


# ==============================
# Validações (regras originais)
# ==============================

def validar_payload(payload: dict, whys_grid: list, causas_linhas: list, acoes_lista: list) -> list:
    erros = []

    obrig_evento_vals = {
        "equipamento": payload.get("equipamento"),
        "secao": payload.get("secao"),
        "data_quebra": str(payload.get("data_quebra")) if payload.get("data_quebra") else None,
        "hora_quebra": str(payload.get("hora_quebra")) if payload.get("hora_quebra") else None,
        "tempo_reparo_h": str(payload.get("tempo_reparo_h")),
        "numero_ordem": payload.get("numero_ordem"),
        "numero_bda": payload.get("numero_bda"),
        "turno": payload.get("turno") if payload.get("turno") else None,
        "time_bda": payload.get("time_bda"),
        "dono_bda": payload.get("dono_bda"),
        "categoria_evento": payload.get("categoria_evento"),
        "componentes": payload.get("componentes"),
        "principio_funcionamento": payload.get("principio_funcionamento"),
        "aconteceu_onde": payload.get("aconteceu_onde"),
        "aconteceu_antes": payload.get("aconteceu_antes"),
        "descricao_reparo": payload.get("descricao_reparo"),
        "modo_falha": payload.get("modo_falha"),
    }
    labels = {
        "equipamento": "Equipamento",
        "secao": "Seção/Local",
        "data_quebra": "Data da quebra",
        "hora_quebra": "Hora da quebra",
        "tempo_reparo_h": "Tempo de reparo (h)",
        "numero_ordem": "Nº Ordem",
        "numero_bda": "Nº BDA",
        "turno": "Turno",
        "time_bda": "Time da BDA",
        "dono_bda": "Dono da BDA",
        "categoria_evento": "Categoria",
        "componentes": "Componentes",
        "principio_funcionamento": "Princípio de funcionamento",
        "aconteceu_onde": "O que aconteceu e onde",
        "aconteceu_antes": "O que aconteceu antes",
        "descricao_reparo": "Descrição do reparo",
        "modo_falha": "Modo da falha",
    }
    erros += all_filled(obrig_evento_vals, labels)

    existe_plano = payload.get("existe_plano")
    if existe_plano == "Sim":
        plan_vals = {
            "plano_sap": payload.get("plano_sap"),
            "descricao_plano": payload.get("descricao_plano"),
            "ultimo_executante": payload.get("ultimo_executante"),
            "periodicidade_dias": str(payload.get("periodicidade_dias")),
            "ultima_realizacao": str(payload.get("ultima_realizacao")) if payload.get("ultima_realizacao") else None,
            "status_plano": payload.get("status_plano"),
        }
        plan_labels = {
            "plano_sap": "Plano SAP",
            "descricao_plano": "Descrição do plano",
            "ultimo_executante": "Último executante",
            "periodicidade_dias": "Periodicidade (dias)",
            "ultima_realizacao": "Última realização",
            "status_plano": "Status do plano",
        }
        erros += all_filled(plan_vals, plan_labels)

    linhas_validas = 0
    for i, row in enumerate(whys_grid, start=1):
        seq_ok = True
        filled_count = 0
        last_idx = 0
        for j in range(1, 6):
            val = (row or {}).get(f"pq{j}")
            prev = (row or {}).get(f"pq{j-1}") if j > 1 else None
            if val and j > 1 and not prev:
                seq_ok = False
            if val:
                filled_count += 1
                last_idx = j
        if not seq_ok:
            erros.append(f"Linha {i}: preencha os porquês em sequência (não pule etapas).")
        if filled_count >= 4:
            linhas_validas += 1

        causa_info = causas_linhas[i - 1] if i - 1 < len(causas_linhas) else {}
        if filled_count > 0 and not (causa_info or {}).get("causa"):
            erros.append(f"Linha {i}: preencha a Causa vinculada ao último Por quê preenchido.")
        else:
            if i - 1 < len(causas_linhas):
                causas_linhas[i - 1]["ultimo_por_que"] = last_idx

    if linhas_validas < 1:
        erros.append("Preencha pelo menos uma linha com no mínimo 4 Por quês.")

    acoes_validas = [a for a in acoes_lista if a.get("descricao") and a.get("responsavel")]

    if existe_plano == "Não":
        auto_desc = "Implementar de padrão/criar plano de manutenção para o conjunto afetado"
        ja_tem_auto = any(auto_desc.lower() in (a.get("descricao", "").lower()) for a in acoes_lista)
        if not ja_tem_auto:
            acoes_lista.append(
                {"descricao": auto_desc, "categoria": "Implementação de padrão", "responsavel": "Definir", "prazo": str(date.today() + timedelta(days=30))}
            )
        acoes_validas = [a for a in acoes_lista if a.get("descricao") and a.get("responsavel")]

    if len(acoes_validas) < 1:
        erros.append("Inclua pelo menos uma ação (descrição e responsável).")

    return erros


# ==============================
# Formulário reutilizável
# ==============================

def formulario_bda(modo: str, dados: dict | None = None, somente_leitura: bool = False) -> dict:
    if modo not in ("novo", "editar"):
        raise ValueError("modo deve ser 'novo' ou 'editar'")

    dados_n = normalizar_dados_bda(dados or {}) if modo == "editar" else {}
    key_prefix = f"{modo}_{dados_n.get('id', '')}" if modo == "editar" else "novo"

    equipamento_init = dados_n.get("equipamento", "")
    secao_init = dados_n.get("secao", "")
    data_quebra_init = dados_n.get("data_quebra", date.today())
    hora_quebra_init = dados_n.get("hora_quebra", datetime.now().time())
    tempo_reparo_init = float(dados_n.get("tempo_reparo_h", 0.0) or 0.0)
    numero_ordem_init = dados_n.get("numero_ordem", "")
    numero_bda_init = dados_n.get("numero_bda", "")
    turno_init = dados_n.get("turno", "")

    time_bda_init = dados_n.get("time_bda", "")
    dono_bda_init = dados_n.get("dono_bda", "")
    categoria_evento_init = dados_n.get("categoria_evento", "Mecânica")

    componentes_init = dados_n.get("componentes", "")
    principio_funcionamento_init = dados_n.get("principio_funcionamento", "")

    aconteceu_onde_init = dados_n.get("aconteceu_onde", "")
    aconteceu_antes_init = dados_n.get("aconteceu_antes", "")
    descricao_reparo_init = dados_n.get("descricao_reparo", "")
    modo_falha_init = dados_n.get("modo_falha", "")

    existe_plano_init = dados_n.get("existe_plano", "Sim")
    plano_sap_init = dados_n.get("plano_sap", "")
    descricao_plano_init = dados_n.get("descricao_plano", "")
    ultimo_executante_init = dados_n.get("ultimo_executante", "")
    periodicidade_dias_init = int(dados_n.get("periodicidade_dias", 0) or 0)
    ultima_realizacao_init = dados_n.get("ultima_realizacao")
    status_plano_init = dados_n.get("status_plano", "No prazo")

    caminho_imagem_existente = dados_n.get("caminho_imagem", "")

    whys_loaded = dados_n.get("cinco_porques_grid", []) if modo == "editar" else []
    causas_loaded = dados_n.get("causas_linhas", []) if modo == "editar" else []
    acoes_loaded = dados_n.get("acoes_lista", []) if modo == "editar" else []

    tab_evento, tab_plano, tab_diag, tab_acoes = st.tabs(["Evento", "Plano Preventivo", "Diagnóstico", "Ações"])

    with tab_evento:
        colA, colB, colC, colD = st.columns(4)
        equipamento = colA.text_input("Equipamento *", value=equipamento_init, key=f"{key_prefix}_equipamento", disabled=somente_leitura)
        secao = colB.text_input("Seção / Local *", value=secao_init, key=f"{key_prefix}_secao", disabled=somente_leitura)
        data_quebra = colC.date_input("Data da quebra *", value=data_quebra_init, key=f"{key_prefix}_data_quebra", disabled=somente_leitura)
        hora_quebra = colD.time_input("Hora da quebra *", value=hora_quebra_init, key=f"{key_prefix}_hora_quebra", disabled=somente_leitura)

        col1, col2, col3, col4 = st.columns(4)
        tempo_reparo_h = col1.number_input(
            "Tempo de reparo (h) *",
            min_value=0.0,
            step=0.25,
            value=float(tempo_reparo_init),
            key=f"{key_prefix}_tempo_reparo_h",
            disabled=somente_leitura,
        )
        numero_ordem = col2.text_input("Nº da Ordem *", value=numero_ordem_init, key=f"{key_prefix}_numero_ordem", disabled=somente_leitura)
        numero_bda = col3.text_input("Nº BDA *", value=numero_bda_init, key=f"{key_prefix}_numero_bda", disabled=somente_leitura)
        turno_opts = ["", "1", "2", "3", "ADM"]
        turno_idx = turno_opts.index(turno_init) if turno_init in turno_opts else 0
        turno = col4.selectbox("Turno *", turno_opts, index=turno_idx, key=f"{key_prefix}_turno", disabled=somente_leitura)

        col5, col6, col7 = st.columns(3)
        time_bda = col5.text_input("Time/Equipe da BDA *", value=time_bda_init, key=f"{key_prefix}_time_bda", disabled=somente_leitura)
        dono_bda = col6.text_input("Dono da BDA *", value=dono_bda_init, key=f"{key_prefix}_dono_bda", disabled=somente_leitura)
        cat_opts = ["Mecânica", "Elétrica", "Instrumentação", "Segurança", "Outros"]
        cat_idx = cat_opts.index(categoria_evento_init) if categoria_evento_init in cat_opts else 0
        categoria_evento = col7.selectbox("Categoria *", cat_opts, index=cat_idx, key=f"{key_prefix}_categoria_evento", disabled=somente_leitura)

        col8, col9 = st.columns(2)
        imagem = col8.file_uploader(
            "Imagem da falha (foto)",
            type=["png", "jpg", "jpeg", "webp"],
            key=f"{key_prefix}_imagem",
            disabled=somente_leitura,
        )
        if modo == "editar" and caminho_imagem_existente and os.path.exists(caminho_imagem_existente):
            col8.caption("Imagem atual:")
            try:
                col8.image(caminho_imagem_existente, use_column_width=True)
            except Exception:
                col8.write(caminho_imagem_existente)

        componentes = col9.text_area(
            "Componentes substituídos (lista) *",
            height=80,
            value=componentes_init,
            key=f"{key_prefix}_componentes",
            disabled=somente_leitura,
        )

        principio_funcionamento = st.text_area(
            "Detalhamento do princípio de funcionamento do conjunto (da falha) *",
            height=100,
            value=principio_funcionamento_init,
            key=f"{key_prefix}_principio_funcionamento",
            disabled=somente_leitura,
        )

        st.subheader("Descrição do Evento")
        aconteceu_onde = st.text_area(
            "O que aconteceu e onde? *",
            height=100,
            value=aconteceu_onde_init,
            key=f"{key_prefix}_aconteceu_onde",
            disabled=somente_leitura,
        )
        aconteceu_antes = st.text_area(
            "O que aconteceu antes da quebra? *",
            height=100,
            value=aconteceu_antes_init,
            key=f"{key_prefix}_aconteceu_antes",
            disabled=somente_leitura,
        )
        descricao_reparo = st.text_area(
            "Descrição da intervenção do reparo *",
            height=100,
            value=descricao_reparo_init,
            key=f"{key_prefix}_descricao_reparo",
            disabled=somente_leitura,
        )
        modo_falha = st.text_input("Modo da falha – frase *", value=modo_falha_init, key=f"{key_prefix}_modo_falha", disabled=somente_leitura)

    with tab_plano:
        existe_plano = st.radio(
            "Existe plano de manutenção para esse conjunto? *",
            ["Sim", "Não"],
            index=0 if existe_plano_init == "Sim" else 1,
            horizontal=True,
            key=f"{key_prefix}_existe_plano",
            disabled=somente_leitura,
        )

        if existe_plano == "Sim":
            colp1, colp2 = st.columns(2)
            plano_sap = colp1.text_input("Plano SAP (nº) *", value=plano_sap_init, key=f"{key_prefix}_plano_sap", disabled=somente_leitura)
            descricao_plano = colp2.text_input("Descrição do plano *", value=descricao_plano_init, key=f"{key_prefix}_descricao_plano", disabled=somente_leitura)

            colp3, colp4, colp5 = st.columns(3)
            ultimo_executante = colp3.text_input("Último executante *", value=ultimo_executante_init, key=f"{key_prefix}_ultimo_executante", disabled=somente_leitura)
            periodicidade_dias = colp4.number_input(
                "Periodicidade (dias) *",
                min_value=0,
                step=1,
                value=int(periodicidade_dias_init),
                key=f"{key_prefix}_periodicidade_dias",
                disabled=somente_leitura,
            )
            ultima_realizacao_default = ultima_realizacao_init or date.today()
            ultima_realizacao = colp5.date_input("Última realização *", value=ultima_realizacao_default, key=f"{key_prefix}_ultima_realizacao", disabled=somente_leitura)

            status_opts = ["No prazo", "Fora do prazo"]
            status_idx = status_opts.index(status_plano_init) if status_plano_init in status_opts else 0
            status_plano = st.selectbox("Status do plano *", status_opts, index=status_idx, key=f"{key_prefix}_status_plano", disabled=somente_leitura)
        else:
            plano_sap = ""
            descricao_plano = ""
            ultimo_executante = ""
            periodicidade_dias = 0
            ultima_realizacao = None
            status_plano = ""
            st.info(
                "Campos do plano ocultados (Não existe plano). Uma ação de implementação de padrão será criada automaticamente ao salvar."
            )

    with tab_diag:
        st.subheader("5 Porquês / Análise detalhada")
        ROWS, COLS = 5, 5
        whys_grid = []
        causas_linhas = []

        whys_loaded = whys_loaded if isinstance(whys_loaded, list) else []
        causas_loaded = causas_loaded if isinstance(causas_loaded, list) else []

        for i in range(ROWS):
            st.markdown(f"**Linha {i+1}**")
            cols = st.columns(COLS + 1)

            loaded_row = whys_loaded[i] if i < len(whys_loaded) and isinstance(whys_loaded[i], dict) else {}
            loaded_causa = causas_loaded[i] if i < len(causas_loaded) and isinstance(causas_loaded[i], dict) else {}

            row_dict = {}
            last_filled_index = 0
            for j in range(1, COLS + 1):
                label = f"Por quê {j} ➜" if j < COLS else f"Por quê {j}"
                init_val = loaded_row.get(f"pq{j}", "") or ""
                val = cols[j - 1].text_area(
                    label,
                    key=f"{key_prefix}_why_{i}_{j}",
                    height=80,
                    value=init_val,
                    placeholder="Descreva o porquê",
                    disabled=somente_leitura,
                )
                row_dict[f"pq{j}"] = val.strip()
                if val.strip():
                    last_filled_index = j
                if j > 1 and row_dict[f"pq{j}"] and not row_dict.get(f"pq{j-1}"):
                    st.warning(f"Preencha o Por quê {j-1} antes do Por quê {j} na linha {i+1}.")

            causa_init = loaded_causa.get("causa", "") or ""
            causa_val = cols[-1].text_area(
                "Causa (ligada ao último porquê)",
                key=f"{key_prefix}_causa_{i}",
                height=80,
                value=causa_init,
                placeholder="Descreva a causa resultante desta linha",
                disabled=somente_leitura,
            )

            whys_grid.append(row_dict)
            causas_linhas.append({"linha": i + 1, "causa": causa_val.strip(), "ultimo_por_que": last_filled_index})
            st.markdown(" ")

    with tab_acoes:
        st.subheader("Ações de contramedidas (mínimo 1)")
        categorias_acoes = ["Melhoria", "Corretiva", "Implementação de padrão", "Poka Yoke"]
        acoes_lista = []
        acoes_loaded = acoes_loaded if isinstance(acoes_loaded, list) else []

        for i in range(5):
            st.markdown(f"**Ação {i+1}**")
            c1, c2, c3, c4 = st.columns([2, 1, 1, 1])

            loaded_acao = acoes_loaded[i] if i < len(acoes_loaded) and isinstance(acoes_loaded[i], dict) else {}
            desc_init = loaded_acao.get("descricao", "") or ""
            cat_init = loaded_acao.get("categoria", categorias_acoes[0])
            resp_init = loaded_acao.get("responsavel", "") or ""
            prazo_init = _parse_date(loaded_acao.get("prazo")) or date.today()

            desc = c1.text_input("Ação de contramedidas", key=f"{key_prefix}_acao_desc_{i}", value=desc_init, disabled=somente_leitura)
            cat_idx = categorias_acoes.index(cat_init) if cat_init in categorias_acoes else 0
            cat = c2.selectbox("Categoria", categorias_acoes, index=cat_idx, key=f"{key_prefix}_acao_cat_{i}", disabled=somente_leitura)
            resp = c3.text_input("Responsável", key=f"{key_prefix}_acao_resp_{i}", value=resp_init, disabled=somente_leitura)
            prazo = c4.date_input("Prazo", value=prazo_init, key=f"{key_prefix}_acao_prazo_{i}", disabled=somente_leitura)

            acoes_lista.append({"descricao": desc.strip(), "categoria": cat, "responsavel": resp.strip(), "prazo": str(prazo) if prazo else None})
            st.divider()

        st.caption("Preencha pelo menos 1 ação.")

    return {
        "equipamento": equipamento,
        "secao": secao,
        "data_quebra": data_quebra,
        "hora_quebra": hora_quebra,
        "tempo_reparo_h": float(tempo_reparo_h),
        "numero_ordem": numero_ordem,
        "numero_bda": numero_bda,
        "turno": turno,
        "aconteceu_onde": aconteceu_onde,
        "aconteceu_antes": aconteceu_antes,
        "descricao_reparo": descricao_reparo,
        "modo_falha": modo_falha,
        "componentes": componentes,
        "principio_funcionamento": principio_funcionamento,
        "time_bda": time_bda,
        "dono_bda": dono_bda,
        "categoria_evento": categoria_evento,
        "existe_plano": existe_plano,
        "plano_sap": plano_sap,
        "descricao_plano": descricao_plano,
        "ultimo_executante": ultimo_executante,
        "periodicidade_dias": int(periodicidade_dias),
        "ultima_realizacao": ultima_realizacao,
        "status_plano": status_plano,
        "cinco_porques_grid": whys_grid,
        "causas_linhas": causas_linhas,
        "acoes_lista": acoes_lista,
        "caminho_imagem": caminho_imagem_existente if modo == "editar" else "",
        "_whys_grid": whys_grid,
        "_causas_linhas": causas_linhas,
        "_acoes_lista": acoes_lista,
        "_imagem_upload": imagem,
    }


# ==============================
# PDF export (inclui foto + auditoria)
# ==============================

def gerar_pdf_bda(dados: dict) -> bytes:
    d = normalizar_dados_bda(dados)
    whys_grid = d.get("cinco_porques_grid", [])
    causas_linhas = d.get("causas_linhas", [])
    acoes_lista = d.get("acoes_lista", [])

    numero_bda = (d.get("numero_bda") or "sem_numero").strip() or "sem_numero"

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
        title=f"BDA {numero_bda}",
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="H1", parent=styles["Heading1"], textColor=colors.HexColor(JDE_BROWN)))
    styles.add(ParagraphStyle(name="H2", parent=styles["Heading2"], textColor=colors.HexColor(JDE_BROWN_MED)))
    styles.add(ParagraphStyle(name="Body", parent=styles["BodyText"], leading=14))

    def p(txt):
        return Paragraph((txt or "").replace("\n", "<br/>") if isinstance(txt, str) else str(txt or ""), styles["Body"])

    story = [Paragraph(f"BDA – {numero_bda}", styles["H1"]), Spacer(1, 0.3 * cm)]

    story.append(Paragraph("Evento", styles["H2"]))
    evento_rows = [
        ["Equipamento", d.get("equipamento", "")],
        ["Seção/Local", d.get("secao", "")],
        ["Data da quebra", str(d.get("data_quebra", ""))],
        ["Hora da quebra", str(d.get("hora_quebra", ""))],
        ["Tempo de reparo (h)", str(d.get("tempo_reparo_h", ""))],
        ["Nº Ordem", d.get("numero_ordem", "")],
        ["Turno", d.get("turno", "")],
        ["Time/Equipe", d.get("time_bda", "")],
        ["Dono da BDA", d.get("dono_bda", "")],
        ["Categoria", d.get("categoria_evento", "")],
        ["Componentes substituídos", d.get("componentes", "")],
        ["Princípio de funcionamento", d.get("principio_funcionamento", "")],
        ["O que aconteceu e onde?", d.get("aconteceu_onde", "")],
        ["O que aconteceu antes da quebra?", d.get("aconteceu_antes", "")],
        ["Descrição do reparo", d.get("descricao_reparo", "")],
        ["Modo da falha", d.get("modo_falha", "")],
        ["Criado por", d.get("criado_por", "")],
        ["Atualizado por", d.get("atualizado_por", "")],
        ["Atualizado em", str(d.get("atualizado_em", "")) if d.get("atualizado_em") else ""],
    ]

    evento_style = TableStyle(
        [
            ("BACKGROUND", (0, 0), (-1, 0), colors.whitesmoke),
            ("TEXTCOLOR", (0, 0), (-1, -1), colors.black),
            ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, colors.HexColor("#f6f3ee")]),
        ]
    )

    t = Table([[p(a), p(b)] for a, b in evento_rows], colWidths=[5 * cm, 11.5 * cm])
    t.setStyle(evento_style)
    story += [t, Spacer(1, 0.4 * cm)]

    caminho_img = d.get("caminho_imagem")
    if caminho_img and isinstance(caminho_img, str) and os.path.exists(caminho_img):
        story.append(Paragraph("Imagem da falha", styles["H2"]))
        try:
            img = RLImage(caminho_img)
            max_w = 16.5 * cm
            max_h = 9.0 * cm
            iw, ih = img.imageWidth, img.imageHeight
            scale = min(max_w / iw, max_h / ih)
            img.drawWidth = iw * scale
            img.drawHeight = ih * scale
            story.append(img)
            story.append(Spacer(1, 0.4 * cm))
        except Exception:
            story.append(Paragraph(f"Arquivo: {caminho_img}", styles["Body"]))
            story.append(Spacer(1, 0.4 * cm))

    story.append(Paragraph("Plano Preventivo", styles["H2"]))
    plano_rows = [
        ["Existe plano?", d.get("existe_plano", "")],
        ["Plano SAP", d.get("plano_sap", "")],
        ["Descrição do plano", d.get("descricao_plano", "")],
        ["Último executante", d.get("ultimo_executante", "")],
        ["Periodicidade (dias)", str(d.get("periodicidade_dias", ""))],
        ["Última realização", str(d.get("ultima_realizacao", "")) if d.get("ultima_realizacao") else ""],
        ["Status do plano", d.get("status_plano", "")],
    ]
    t2 = Table([[p(a), p(b)] for a, b in plano_rows], colWidths=[5 * cm, 11.5 * cm])
    t2.setStyle(evento_style)
    story += [t2, Spacer(1, 0.4 * cm)]

    story.append(Paragraph("Diagnóstico", styles["H2"]))
    story.append(Paragraph("5 Porquês e Causas por linha", styles["Body"]))

    diag_header = ["Linha", "Por quê 1", "Por quê 2", "Por quê 3", "Por quê 4", "Por quê 5", "Causa"]
    diag_rows = [diag_header]
    for i in range(5):
        row = whys_grid[i] if i < len(whys_grid) and isinstance(whys_grid[i], dict) else {}
        causa = causas_linhas[i].get("causa") if i < len(causas_linhas) and isinstance(causas_linhas[i], dict) else ""
        diag_rows.append([str(i + 1), row.get("pq1", ""), row.get("pq2", ""), row.get("pq3", ""), row.get("pq4", ""), row.get("pq5", ""), causa or ""])

    t3 = Table([[p(c) for c in r] for r in diag_rows], colWidths=[1.0 * cm, 2.6 * cm, 2.6 * cm, 2.6 * cm, 2.6 * cm, 2.6 * cm, 2.7 * cm])
    t3.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#efe7dc")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
            ]
        )
    )
    story += [t3, Spacer(1, 0.4 * cm)]

    story.append(Paragraph("Ações", styles["H2"]))
    acoes_header = ["#", "Descrição", "Categoria", "Responsável", "Prazo"]
    acoes_rows = [acoes_header]
    for i, acao in enumerate(acoes_lista or [], start=1):
        if isinstance(acao, dict):
            acoes_rows.append([str(i), acao.get("descricao", ""), acao.get("categoria", ""), acao.get("responsavel", ""), acao.get("prazo", "")])
    if len(acoes_rows) == 1:
        acoes_rows.append(["-", "-", "-", "-", "-"])

    t4 = Table([[p(c) for c in r] for r in acoes_rows], colWidths=[0.8 * cm, 7.5 * cm, 3.0 * cm, 3.0 * cm, 2.2 * cm])
    t4.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#efe7dc")),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
            ]
        )
    )
    story.append(t4)

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes


# ==============================
# INSERT / UPDATE (com auditoria)
# ==============================

DB_COLUMNS = [
    "equipamento",
    "secao",
    "data_quebra",
    "hora_quebra",
    "tempo_reparo_h",
    "numero_ordem",
    "numero_bda",
    "turno",
    "centro_custo",
    "aconteceu_onde",
    "aconteceu_antes",
    "descricao_reparo",
    "modo_falha",
    "acoes_corretivas",
    "responsavel_corretiva",
    "quando_corretiva",
    "plano_sap",
    "descricao_plano",
    "responsavel_plano",
    "periodicidade_dias",
    "ultima_realizacao",
    "caminho_imagem",
    "criticidade",
    "categoria",
    "classificacao",
    "causa_raiz",
    "cinco_porques",
    "componentes",
    "custo_pecas",
    "custo_mo",
    "time_bda",
    "dono_bda",
    "categoria_evento",
    "cinco_porques_grid",
    "acoes_lista",
    "ultimo_executante",
    "status_plano",
    "existe_plano",
    "principio_funcionamento",
    "causas_linhas",
    "criado_por",
    "atualizado_por",
    "atualizado_em",
]


def _montar_db_payload(payload: dict, caminho_imagem: str | None, user_tag: str, is_update: bool) -> dict:
    now_ts = datetime.now().isoformat(sep=" ", timespec="seconds")
    db_payload = {
        "equipamento": payload.get("equipamento"),
        "secao": payload.get("secao"),
        "data_quebra": str(payload.get("data_quebra")) if payload.get("data_quebra") else None,
        "hora_quebra": str(payload.get("hora_quebra")) if payload.get("hora_quebra") else None,
        "tempo_reparo_h": float(payload.get("tempo_reparo_h") or 0.0),
        "numero_ordem": payload.get("numero_ordem"),
        "numero_bda": payload.get("numero_bda"),
        "turno": payload.get("turno"),
        "centro_custo": None,
        "aconteceu_onde": payload.get("aconteceu_onde"),
        "aconteceu_antes": payload.get("aconteceu_antes"),
        "descricao_reparo": payload.get("descricao_reparo"),
        "modo_falha": payload.get("modo_falha"),
        "acoes_corretivas": None,
        "responsavel_corretiva": None,
        "quando_corretiva": None,
        "plano_sap": payload.get("plano_sap"),
        "descricao_plano": payload.get("descricao_plano"),
        "responsavel_plano": None,
        "periodicidade_dias": int(payload.get("periodicidade_dias") or 0),
        "ultima_realizacao": str(payload.get("ultima_realizacao")) if payload.get("ultima_realizacao") else None,
        "caminho_imagem": caminho_imagem,
        "criticidade": None,
        "categoria": None,
        "classificacao": None,
        "causa_raiz": None,
        "cinco_porques": None,
        "componentes": payload.get("componentes"),
        "custo_pecas": None,
        "custo_mo": None,
        "time_bda": payload.get("time_bda"),
        "dono_bda": payload.get("dono_bda"),
        "categoria_evento": payload.get("categoria_evento"),
        "cinco_porques_grid": json.dumps(payload.get("_whys_grid") or payload.get("cinco_porques_grid") or [], ensure_ascii=False),
        "acoes_lista": json.dumps(payload.get("_acoes_lista") or payload.get("acoes_lista") or [], ensure_ascii=False),
        "ultimo_executante": payload.get("ultimo_executante"),
        "status_plano": payload.get("status_plano"),
        "existe_plano": payload.get("existe_plano"),
        "principio_funcionamento": payload.get("principio_funcionamento"),
        "causas_linhas": json.dumps(payload.get("_causas_linhas") or payload.get("causas_linhas") or [], ensure_ascii=False),
        "criado_por": None if is_update else user_tag,
        "atualizado_por": user_tag if is_update else None,
        "atualizado_em": now_ts if is_update else None,
    }
    return {k: db_payload.get(k) for k in DB_COLUMNS}


def inserir_bda(payload: dict, user_tag: str) -> None:
    imagem_upload = payload.get("_imagem_upload")
    img_path = salvar_imagem(imagem_upload) if imagem_upload is not None else None
    db_payload = _montar_db_payload(payload, img_path, user_tag=user_tag, is_update=False)
    cols = ",".join(db_payload.keys())
    qs = ":" + ",:".join(db_payload.keys())
    cur.execute(f"INSERT INTO bda ({cols}) VALUES ({qs})", db_payload)
    conn.commit()


def atualizar_bda(bda_id: int, payload: dict, user_tag: str, caminho_imagem_atual: str | None = None) -> None:
    imagem_upload = payload.get("_imagem_upload")
    img_path = caminho_imagem_atual if imagem_upload is None else salvar_imagem(imagem_upload)
    db_payload = _montar_db_payload(payload, img_path, user_tag=user_tag, is_update=True)
    set_clause = ", ".join([f"{k} = :{k}" for k in db_payload.keys()])
    db_payload["id"] = int(bda_id)
    cur.execute(f"UPDATE bda SET {set_clause} WHERE id = :id", db_payload)
    conn.commit()


# ==============================
# Sidebar e navegação
# ==============================

def render_sidebar():
    u = current_user()
    with st.sidebar:
        logo_candidates = ["jde_logo.png", "logo.png", os.path.join("assets", "jde_logo.png")]
        for p in logo_candidates:
            if os.path.exists(p):
                st.image(p, use_column_width=True)
                break
        else:
            st.markdown("""\n**JDE**\n\nManutenção · BDA\n""")

        st.markdown("---")
        st.write(f"👤 **{u.get('email','')}**")
        st.caption(f"Perfil: **{u.get('role','')}**")
        st.caption(
            "Permissões: "
            + ("Registrar" if u.get("pode_registrar") else "")
            + (" · Editar" if u.get("pode_editar") else " · Somente leitura")
        )

        if st.button("Sair", use_container_width=True):
            st.session_state["auth"] = None
            st.rerun()

        st.markdown("---")
        paginas = ["Dashboard", "Registrar BDA", "Consulta/Editar"]
        pagina = st.radio("Navegação", paginas, index=0)
        st.caption("Tema JDE – campos e regras conforme solicitação.")
        return pagina


# ==============================
# Páginas
# ==============================

def pagina_registrar():
    u = current_user()
    if not u.get("pode_registrar"):
        st.error("Seu perfil não tem permissão para registrar BDAs.")
        st.stop()

    st.header("Registro de BDA – JDE")
    payload = formulario_bda(modo="novo")

    st.markdown("---")
    if st.button("Salvar BDA", use_container_width=True):
        erros = validar_payload(payload, payload.get("_whys_grid", []), payload.get("_causas_linhas", []), payload.get("_acoes_lista", []))
        if erros:
            for e in erros:
                st.error(e)
            st.stop()

        user_tag = f"{u.get('email')} ({u.get('role')})"
        inserir_bda(payload, user_tag=user_tag)
        st.success("BDA salva com sucesso!")
        st.balloons()


def pagina_consulta_editar():
    u = current_user()
    pode_editar = bool(u.get("pode_editar"))

    st.header("Consulta de BDAs – JDE")

    colf1, colf2, colf3 = st.columns([1, 1, 1])
    data_ini = colf1.date_input("De", value=date.today() - timedelta(days=30), key="consulta_data_ini")
    data_fim = colf2.date_input("Até", value=date.today(), key="consulta_data_fim")
    filtro_equip = colf3.text_input("Equipamento contém", "", key="consulta_filtro_equip")

    sql = "SELECT * FROM bda WHERE date(data_quebra) BETWEEN date(?) AND date(?)"
    params = [str(data_ini), str(data_fim)]
    if filtro_equip:
        sql += " AND LOWER(equipamento) LIKE ?"
        params.append(f"%{filtro_equip.lower()}%")

    df = df_from_query(sql, params)
    st.caption(f"{len(df)} registros")

    if df.empty:
        st.info("Nenhum registro encontrado.")
        return

    for c in ["data_quebra", "ultima_realizacao", "created_at", "atualizado_em"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    st.dataframe(
        df[[c for c in df.columns if c not in ("acoes_corretivas", "responsavel_corretiva", "quando_corretiva", "criticidade", "classificacao", "categoria", "custo_pecas", "custo_mo")]],
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Abrir / Editar / Exportar"):
        if not pode_editar:
            st.warning("Perfil TÉCNICO: edição em modo SOMENTE LEITURA. Você pode registrar novas BDAs.")

        colsel1, colsel2 = st.columns([1, 2])
        ids = df["id"].tolist()
        sel_id = colsel1.selectbox("Selecionar por ID", ids, key="sel_bda_id")

        bda_nums = sorted({str(x) for x in df["numero_bda"].fillna("").tolist() if str(x).strip()})
        sel_num = colsel2.selectbox("Ou selecionar por Nº BDA", [""] + bda_nums, index=0, key="sel_bda_num")
        if sel_num:
            row_df = df[df["numero_bda"].astype(str) == sel_num]
            if not row_df.empty:
                sel_id = int(row_df.iloc[0]["id"])

        row_db = df[df["id"] == sel_id].iloc[0].to_dict()
        st.subheader(f"BDA ID {sel_id} – Nº {row_db.get('numero_bda', '')}")

        payload_edit = formulario_bda(modo="editar", dados=row_db, somente_leitura=(not pode_editar))

        cbtn1, cbtn2, cbtn3 = st.columns([1, 1, 1])

        if pode_editar:
            if cbtn1.button("Salvar alterações", use_container_width=True, key=f"btn_save_{sel_id}"):
                erros = validar_payload(payload_edit, payload_edit.get("_whys_grid", []), payload_edit.get("_causas_linhas", []), payload_edit.get("_acoes_lista", []))
                if erros:
                    for e in erros:
                        st.error(e)
                    st.stop()

                user_tag = f"{u.get('email')} ({u.get('role')})"
                atualizar_bda(sel_id, payload_edit, user_tag=user_tag, caminho_imagem_atual=row_db.get("caminho_imagem"))
                st.success("Alterações salvas com sucesso!")
                st.rerun()
        else:
            cbtn1.button("Salvar alterações", use_container_width=True, disabled=True)

        if cbtn2.button("Gerar PDF", use_container_width=True, key=f"btn_pdf_{sel_id}"):
            df_one = df_from_query("SELECT * FROM bda WHERE id = ?", [sel_id])
            dados_pdf = df_one.iloc[0].to_dict() if not df_one.empty else row_db
            pdf_bytes = gerar_pdf_bda(dados_pdf)
            nome_pdf = f"bda_{(dados_pdf.get('numero_bda') or sel_id)}.pdf"
            st.download_button(
                "Exportar PDF",
                data=pdf_bytes,
                file_name=nome_pdf,
                mime="application/pdf",
                use_container_width=True,
                key=f"dl_pdf_{sel_id}",
            )

        cbtn3.download_button(
            "Exportar CSV (filtro)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"bda_{data_ini}_{data_fim}.csv",
            mime="text/csv",
            use_container_width=True,
        )


def pagina_dashboard():
    st.header("Dashboard de Manutenção (BDA) – JDE")

    colf1, colf2, colf3 = st.columns([1, 1, 1])
    data_ini = colf1.date_input("De", value=date.today() - timedelta(days=90), key="dash_data_ini")
    data_fim = colf2.date_input("Até", value=date.today(), key="dash_data_fim")
    filtro_equip = colf3.text_input("Equipamento contém", "", key="dash_filtro_equip")

    sql = "SELECT * FROM bda WHERE date(data_quebra) BETWEEN date(?) AND date(?)"
    params = [str(data_ini), str(data_fim)]
    if filtro_equip:
        sql += " AND LOWER(equipamento) LIKE ?"
        params.append(f"%{filtro_equip.lower()}%")

    df = df_from_query(sql, params)
    if df.empty:
        st.info("Sem dados para o período/critério.")
        return

    if "tempo_reparo_h" in df:
        df["tempo_reparo_h"] = pd.to_numeric(df["tempo_reparo_h"], errors="coerce").fillna(0.0)
    if "data_quebra" in df:
        df["data_quebra"] = pd.to_datetime(df["data_quebra"], errors="coerce")

    falhas = len(df)
    tempo_reparo_total = float(df["tempo_reparo_h"].sum())
    mttr = tempo_reparo_total / falhas if falhas > 0 else np.nan

    horas_periodo = ((pd.to_datetime(str(data_fim)) - pd.to_datetime(str(data_ini))).days + 1) * 24
    tempo_parada = float(df.get("tempo_reparo_h", pd.Series(dtype=float)).sum())
    tempo_operacao = horas_periodo - tempo_parada if horas_periodo is not None else np.nan
    mtbf = (tempo_operacao / falhas) if falhas > 0 and tempo_operacao >= 0 else np.nan
    disponibilidade = (mtbf / (mtbf + mttr)) if (not np.isnan(mtbf) and not np.isnan(mttr) and (mtbf + mttr) > 0) else np.nan

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Falhas", f"{falhas}")
    k2.metric("MTTR (h)", f"{mttr:.2f}" if not np.isnan(mttr) else "-")
    k3.metric("MTBF (h)", f"{mtbf:.2f}" if not np.isnan(mtbf) else "-")
    k4.metric("Disponibilidade", f"{(disponibilidade*100):.1f}%" if not np.isnan(disponibilidade) else "-")

    st.markdown("---")
    colg1, colg2 = st.columns(2)

    if "equipamento" in df and not df["equipamento"].isna().all():
        top_eq = df.groupby("equipamento", dropna=False).size().reset_index(name="falhas").sort_values("falhas", ascending=False).head(10)
        chart1 = alt.Chart(top_eq).mark_bar(color=JDE_TERRACOTTA).encode(
            x=alt.X("falhas:Q", title="Falhas"),
            y=alt.Y("equipamento:N", sort="-x", title="Equipamento"),
        ).properties(height=300)
        colg1.subheader("Top 10 Equipamentos por falhas")
        colg1.altair_chart(chart1, use_container_width=True)

    if "categoria_evento" in df and not df["categoria_evento"].isna().all():
        por_cat = df.groupby("categoria_evento", dropna=False).size().reset_index(name="falhas").sort_values("falhas", ascending=False)
        chart2 = alt.Chart(por_cat).mark_bar(color=JDE_BROWN_MED).encode(
            x=alt.X("falhas:Q", title="Falhas"),
            y=alt.Y("categoria_evento:N", sort="-x", title="Categoria"),
        ).properties(height=300)
        colg2.subheader("Falhas por categoria (evento)")
        colg2.altair_chart(chart2, use_container_width=True)

    st.subheader("Linha do tempo de falhas")
    timeline = df.set_index("data_quebra").sort_index().assign(Falhas=1)["Falhas"].resample("D").sum().reset_index()
    line = alt.Chart(timeline).mark_line(color=JDE_TEAL).encode(
        x=alt.X("data_quebra:T", title="Data"),
        y=alt.Y("Falhas:Q", title="Falhas/dia"),
    ).properties(height=220)
    st.altair_chart(line, use_container_width=True)

    st.subheader("Tabela detalhada")
    st.dataframe(df.sort_values("data_quebra", ascending=False), use_container_width=True, hide_index=True)
    st.caption("*MTBF calculado como horas do período menos horas de parada (aproximação). Pode ser refinado por equipamento.")


# ==============================
# Router
# ==============================

pagina = render_sidebar()

if pagina == "Dashboard":
    pagina_dashboard()
elif pagina == "Registrar BDA":
    pagina_registrar()
elif pagina == "Consulta/Editar":
    pagina_consulta_editar()
