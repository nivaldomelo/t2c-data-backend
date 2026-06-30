from __future__ import annotations

import datetime as _dt
import math
import re
import textwrap
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
from xml.sax.saxutils import escape as xml_escape

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "docs"
MD_PATH = OUT_DIR / "t2c_data_manual.md"
DOCX_PATH = OUT_DIR / "t2c_data_manual.docx"
PDF_PATH = OUT_DIR / "t2c_data_manual.pdf"

TODAY = _dt.date.today().strftime("%d/%m/%Y")


@dataclass
class ParagraphBlock:
    text: str


@dataclass
class BulletBlock:
    items: list[str]


@dataclass
class TableBlock:
    title: str
    headers: list[str]
    rows: list[list[str]]
    widths: list[float] | None = None


@dataclass
class ScreenBlock:
    title: str
    routes: list[str]
    summary: str
    rows: list[tuple[str, str]]


def screen(title: str, routes: Sequence[str], summary: str, rows: Sequence[tuple[str, str]]) -> ScreenBlock:
    return ScreenBlock(title=title, routes=list(routes), summary=summary, rows=list(rows))


def paragraph(text: str) -> ParagraphBlock:
    return ParagraphBlock(text=text)


def bullets(items: Sequence[str]) -> BulletBlock:
    return BulletBlock(items=list(items))


def table(title: str, headers: Sequence[str], rows: Sequence[Sequence[str]], widths: Sequence[float] | None = None) -> TableBlock:
    return TableBlock(title=title, headers=list(headers), rows=[list(row) for row in rows], widths=list(widths) if widths else None)


def build_document_elements() -> list[object]:
    elements: list[object] = []
    elements.append(
        {
            "type": "cover",
            "title": "t2c_data",
            "subtitle": "Manual corporativo de produto, operação e governança",
            "metadata": [
                ("Versão", TODAY),
                ("Status", "Produção"),
                ("Público-alvo", "Liderança, governança, operação e usuários finais"),
                ("Escopo", "Catálogo, DQ, stewardship, lineage, cockpit, auditoria e administração"),
            ],
        }
    )

    elements.extend(
        [
            {"type": "heading", "level": 1, "text": "Visão executiva"},
            paragraph(
                "O t2c_data é a camada operacional e executiva do catálogo corporativo. A plataforma organiza inventário,"
                " uso, governança e priorização para que times de dados trabalhem com um mapa claro dos ativos, dos riscos"
                " e das decisões pendentes."
            ),
            paragraph(
                "Na prática, a ferramenta conecta descoberta, exploração, qualidade, certificação, linhagem, stewardship,"
                " incidentes, auditoria e configuração. O objetivo não é apenas registrar ativos, mas transformar o catálogo"
                " em uma rotina de decisão e manutenção contínua da maturidade dos dados."
            ),
            {"type": "heading", "level": 1, "text": "Proposta de valor"},
            bullets(
                [
                    "Centralizar a visão executiva do catálogo, destacando cobertura, owners, qualidade, certificação, linhagem e sinais operacionais.",
                    "Reduzir atrito entre exploração técnica e decisão de governança, com fluxos claros de pendência, aprovação e rastreabilidade.",
                    "Permitir operação diária com base em priorização real: o que está crítico, o que precisa revisão e o que pode seguir em acompanhamento.",
                    "Apoiar liderança e times de dados com um vocabulário comum para maturidade, risco e responsabilidade.",
                ]
            ),
            {"type": "heading", "level": 1, "text": "Perfis e responsabilidades"},
            table(
                "Matriz de acesso e responsabilidade",
                ["Perfil", "Papel principal", "Capacidade principal"],
                [
                    ["Admin", "Gestão total", "Configuração, edição, aprovação, administração e visibilidade ampla."],
                    ["Editor", "Operação funcional", "Leitura e execução operacional, sem acesso administrativo e sem escrita em configuração."],
                    ["Visualizador", "Consulta", "Acesso somente leitura para análise e acompanhamento."],
                    ["Stewardship", "Decisão e revisão", "Leitura ampla, abertura e aprovação de solicitações de stewardship."],
                    ["Data owner", "Responsável de dados", "Leitura ampla e participação nas decisões de stewardship como owner."],
                ],
                widths=[0.18, 0.32, 0.5],
            ),
            paragraph(
                "Regra prática: admin mantém o controle da plataforma; editor atua no trabalho operacional; visualizador observa;"
                " stewardship e data owner conduzem revisão e aprovação de decisões sobre ativos."
            ),
            {"type": "heading", "level": 1, "text": "Navegação principal"},
            table(
                "Mapa da interface",
                ["Área", "Telas principais", "Uso esperado"],
                [
                    ["Home e dashboard", "Resumo executivo do catálogo, Dashboard, campanhas automáticas", "Acompanhar maturidade, risco e tendências do catálogo."],
                    ["Busca e exploração", "Search, Explorer, detalhe de ativo, detalhe de coluna", "Encontrar ativos, entender metadados e navegar por contexto."],
                    ["Governança", "Data Quality, pendências, stewardship, certificação, termos, tags, responsáveis", "Registrar decisões, corrigir lacunas e estruturar ownership."],
                    ["Operação", "Cockpit operacional, ingestão, datasources, scan runs, incidents", "Monitorar ingestão, falhas e sinais operacionais relevantes."],
                    ["Administração", "Configuração, usuários, papéis, permissões e aliases de busca", "Controlar parâmetros, acesso e experiência administrativa."],
                ],
                widths=[0.2, 0.42, 0.38],
            ),
            {"type": "heading", "level": 1, "text": "Jornada de uso recomendada"},
            bullets(
                [
                    "Comece pelo Resumo executivo do catálogo para entender cobertura, freshness, riscos e o que exige atenção imediata.",
                    "Use Search e Explorer para localizar ativos, validar metadados e abrir o detalhe técnico certo.",
                    "Complete e priorize ownership, tags, termos, certificação e DQ antes de tratar a governança como madura.",
                    "Acompanhe pendências, stewardship, inbox e cockpit para evitar que problemas operacionais ou de qualidade fiquem invisíveis.",
                    "Finalize com auditoria, configuração e administração para manter o ambiente coerente e auditável em produção.",
                ]
            ),
            {"type": "heading", "level": 1, "text": "Documentação por tela"},
        ]
    )

    screens: list[ScreenBlock] = [
        screen(
            "Login",
            ["/login"],
            "Porta de entrada autenticada da plataforma. Define a sessão, aplica RBAC e direciona o usuário para a área compatível com seu perfil.",
            [
                ("O que é", "Tela pública de autenticação do t2c_data, usada para iniciar sessão com identidade corporativa e receber o contexto de acesso correto."),
                ("Problema que resolve", "Evita acesso indevido e padroniza a entrada no sistema, garantindo que o usuário caia na rota adequada para seu perfil."),
                ("Quem usa", "Todos os perfis que acessam o produto em produção."),
                ("Quando usar", "No início da jornada, em reautenticações ou ao trocar de usuário."),
                ("Como usar", "Informar credenciais válidas ou o método corporativo configurado; após a validação, a aplicação redireciona para a página de destino."),
                ("Decisões suportadas", "Validação de identidade, sessão ativa e perfil aplicável na camada de navegação."),
                ("Boas práticas", "Usar contas nominais e manter o perfil correto para preservar trilhas de auditoria e permissões consistentes."),
                ("Erros comuns / cuidados", "Evitar compartilhamento de credenciais e acesso direto por URL sem sessão, que resulta em bloqueio."),
                ("Contribuição para maturidade", "Garante controle de acesso e base confiável para RBAC, auditoria e governança operacional."),
            ],
        ),
        screen(
            "Perfil",
            ["/me/profile"],
            "Área de conta do usuário para revisar identidade, preferências, senha e contexto de acesso.",
            [
                ("O que é", "Página pessoal para revisão de dados da conta, sessão e configurações de uso."),
                ("Problema que resolve", "Permite ao usuário cuidar da própria sessão e reduzir dependência do time de administração para tarefas simples."),
                ("Quem usa", "Todos os perfis autenticados."),
                ("Quando usar", "Ao atualizar dados pessoais, senha ou revisar o contexto de sessão."),
                ("Como usar", "Acessar a tela de perfil e concluir as ações permitidas pelo próprio usuário."),
                ("Decisões suportadas", "Troca de senha, revisão de preferências e controle do estado da conta."),
                ("Boas práticas", "Manter informações atualizadas e revisar preferências depois de mudanças no ambiente ou no time."),
                ("Erros comuns / cuidados", "Não utilizar o perfil como área administrativa; as alterações devem se restringir ao escopo da conta."),
                ("Contribuição para maturidade", "Fortalece autonomia do usuário e reduz ruído operacional em tarefas pessoais de acesso."),
            ],
        ),
        screen(
            "Resumo executivo do catálogo",
            ["/"],
            "Visão consolidada da saúde e da maturidade dos ativos de dados, útil para liderança, governança e priorização diária.",
            [
                ("O que é", "Painel de entrada do catálogo com indicadores de cobertura, owners, qualidade, certificação, linhagem, atualização e sinais operacionais."),
                ("Problema que resolve", "Evita que a liderança precise navegar por várias telas para entender o estado do catálogo e as principais pendências."),
                ("Quem usa", "Liderança, governança, data owners e operadores que precisam de leitura executiva."),
                ("Quando usar", "No início do dia, em reuniões de acompanhamento e para priorizar ações de melhoria."),
                ("Como usar", "Interpretar os cards e tendências, abrir os atalhos de apoio e usar os sinais para escolher o próximo foco de trabalho."),
                ("Decisões suportadas", "Priorização de risco, cobertura de catálogo, backlog de governança e foco em maturidade."),
                ("Boas práticas", "Tratar os números como leitura de gestão e não como fim em si; cada card deve direcionar uma ação concreta."),
                ("Erros comuns / cuidados", "Não ler o resumo como relatório estático; ele reflete o estado atual e precisa ser acompanhado continuamente."),
                ("Contribuição para maturidade", "Sintetiza a visão do catálogo e conecta execução operacional com governança de alto nível."),
            ],
        ),
        screen(
            "Dashboard",
            ["/dashboard", "/dashboard/campaigns/[campaignKey]"],
            "Painel analítico para leitura de tendência, maturidade e campanhas automáticas com foco em priorização.",
            [
                ("O que é", "Área analítica com KPIs, gráficos, ranking de risco e campanhas automáticas para dirigir a atenção do time."),
                ("Problema que resolve", "Consolida sinais de maturidade e risco em uma visão comparável, útil para operação e gestão."),
                ("Quem usa", "Liderança, governança, times de dados e operação."),
                ("Quando usar", "Em rotinas de acompanhamento, planejamento e revisão de ações priorizadas."),
                ("Como usar", "Filtrar o recorte, interpretar os painéis e abrir campanhas automáticas quando houver necessidade de ação recorrente."),
                ("Decisões suportadas", "Priorização de backlog, acompanhamento de tendências e abertura de campanhas de melhoria."),
                ("Boas práticas", "Usar filtros consistentes entre reuniões e manter os painéis alinhados ao recorte operacional real."),
                ("Erros comuns / cuidados", "Evitar usar um único KPI isolado como se resumisse todo o estado do catálogo."),
                ("Contribuição para maturidade", "Transforma o catálogo em ferramenta de gestão de risco e de evolução contínua."),
            ],
        ),
        screen(
            "Search",
            ["/search", "/search/aliases"],
            "Busca global para encontrar rapidamente ativos, termos e contextos relevantes sem navegar por árvore inteira.",
            [
                ("O que é", "Busca textual com respostas rápidas e links diretos para o contexto do ativo, inclusive resultados enriquecidos."),
                ("Problema que resolve", "Reduz o tempo para localizar ativos quando o usuário já tem nome, parte do nome ou contexto parcial."),
                ("Quem usa", "Todos os perfis que precisam localizar informação com rapidez."),
                ("Quando usar", "Quando o objetivo é achar um ativo, uma tabela, um termo ou uma visão ligada a ele."),
                ("Como usar", "Pesquisar por nome, schema, descrição, alias ou conteúdo relacionado e abrir o resultado com melhor contexto."),
                ("Decisões suportadas", "Descoberta de ativos, análise de contexto e validação rápida de cobertura da base."),
                ("Boas práticas", "Manter aliases úteis, títulos consistentes e descrições claras para melhorar a recuperabilidade."),
                ("Erros comuns / cuidados", "Não depender apenas de busca se o objetivo for análise profunda; nesse caso, o Explorer é mais adequado."),
                ("Contribuição para maturidade", "Aumenta a encontrabilidade e reduz a fricção de acesso ao catálogo."),
            ],
        ),
        screen(
            "Explorer",
            ["/explorer", "/tables/[id]"],
            "Núcleo de exploração do catálogo para navegar em ativos, entender metadados e abrir detalhes técnicos.",
            [
                ("O que é", "Árvore e detalhe de ativos com visão técnica e funcional, incluindo metadados, relacionamento e contexto operacional."),
                ("Problema que resolve", "Permite entender rapidamente um ativo sem depender de múltiplos sistemas ou de conhecimento de memória."),
                ("Quem usa", "Data owners, stewardship, analistas, operação e liderança técnica."),
                ("Quando usar", "Ao investigar um ativo, revisar metadados, conferir colunas, tags, certificação ou sinais de qualidade."),
                ("Como usar", "Buscar o ativo, abrir o detalhe, navegar por abas e seguir para a visão de coluna, ownership, lineage e contexto operacional."),
                ("Decisões suportadas", "Validação de metadados, análise de criticidade, correlação operacional e descoberta de lacunas de documentação."),
                ("Boas práticas", "Manter nome, descrição, tags e owner coerentes; isso melhora toda a navegação e a priorização automática."),
                ("Erros comuns / cuidados", "Evitar abrir detalhe sem contexto de negócio; o Explorer funciona melhor quando o usuário já sabe o objetivo da análise."),
                ("Contribuição para maturidade", "É a porta principal para tornar o catálogo útil no dia a dia e não apenas um inventário passivo."),
            ],
        ),
        screen(
            "Detalhes de ativo e detalhe de coluna",
            ["/tables/[id]"],
            "Visão de profundidade do ativo e de suas colunas, com metadados, qualidade, lineage e leitura técnica mais rica.",
            [
                ("O que é", "Camada de detalhe do Explorer para ler propriedades do ativo e entender o significado técnico de cada coluna."),
                ("Problema que resolve", "Evita depender de planilhas ou documentação externa para entender campos, tipos, obrigatoriedade e contexto."),
                ("Quem usa", "Analistas, stewardship, data owners, governança e times técnicos."),
                ("Quando usar", "Ao revisar um ativo recém-inventariado, confirmar padrões de coluna ou preparar uma decisão de governança."),
                ("Como usar", "Abrir o ativo no Explorer, navegar até a aba de colunas e inspecionar tipo, descrição, defaults, chaves e indicadores."),
                ("Decisões suportadas", "Classificação de uso, adequação de nomenclatura, revisão de completude e entendimento do contrato técnico."),
                ("Boas práticas", "Usar o tipo de dado como referência principal e completar com descrição, classificação e relações de lineage."),
                ("Erros comuns / cuidados", "Não tratar o detalhe técnico como documentação final de negócio; ele deve ser validado com o owner."),
                ("Contribuição para maturidade", "Aumenta a confiabilidade técnica do catálogo e reduz ambiguidade na leitura dos ativos."),
            ],
        ),
        screen(
            "Fontes de dados, ingestão e histórico de scans",
            ["/datasources", "/ops/ingestion", "/scan-runs"],
            "Área de origem e execução de scans, com agendamento, histórico, saúde operacional e ciclo de atualização.",
            [
                ("O que é", "Cadastro das fontes e visão operacional dos scans que coletam inventário, metadados e estado de atualização."),
                ("Problema que resolve", "Permite controlar origem dos dados, periodicidade de captura e histórico de execução sem depender de processos manuais dispersos."),
                ("Quem usa", "Operação, administradores, times de dados e responsáveis por integração."),
                ("Quando usar", "Ao cadastrar uma fonte, revisar status de ingestão, reprocessar uma execução ou investigar falhas de atualização."),
                ("Como usar", "Abrir a fonte, verificar configuração, acionar o scan e acompanhar o histórico de execuções e falhas."),
                ("Decisões suportadas", "Qual fonte está ativa, qual está degradada e onde a coleta de metadados precisa de intervenção."),
                ("Boas práticas", "Manter periodicidade adequada, evitar fontes sem owner e revisar falhas recorrentes com prioridade operacional."),
                ("Erros comuns / cuidados", "Não misturar configuração da fonte com o inventário já coletado; cada scan deve preservar histórico e rastreabilidade."),
                ("Contribuição para maturidade", "Garante alimentação contínua do catálogo e reduz lacunas de atualização e cobertura."),
            ],
        ),
        screen(
            "Responsáveis de dados",
            ["/data-owners"],
            "Cadastro de responsáveis e seus ativos, orientado para leitura clara, relação com Explorer e apoio à stewardship.",
            [
                ("O que é", "Área para organizar responsáveis de dados, suas áreas e o conjunto de tabelas/ativos associados."),
                ("Problema que resolve", "Tira a responsabilidade do campo informal e coloca o ownership em um espaço consultável e navegável."),
                ("Quem usa", "Data owners, stewardship, governança e liderança de dados."),
                ("Quando usar", "Ao definir responsável por um ativo, revisar cobertura de ownership ou descobrir quem responde por um domínio."),
                ("Como usar", "Buscar o responsável, abrir os detalhes, validar a lista de ativos e navegar para Explorer quando necessário."),
                ("Decisões suportadas", "Atribuição de owner, revisão de cobertura de responsabilidade e priorização de pendências por dono."),
                ("Boas práticas", "Manter nome, área e ativos associados atualizados; isso melhora stewardship, busca e priorização automática."),
                ("Erros comuns / cuidados", "Não usar a tela como cadastro solto de pessoas sem vínculo com ativos reais."),
                ("Contribuição para maturidade", "É um dos pilares de governança: sem owner, o catálogo perde capacidade de decisão e acompanhamento."),
            ],
        ),
        screen(
            "Termos e dicionário de dados",
            ["/glossary", "/governance/dictionary"],
            "Vocabulário de negócio e dicionário técnico para padronizar significados, nomes e descrições.",
            [
                ("O que é", "Base de termos aprovados e suporte ao dicionário técnico que ajuda a explicar colunas, conceitos e nomes."),
                ("Problema que resolve", "Evita ambiguidade entre equipes e cria uma linguagem comum para governança, operação e negócio."),
                ("Quem usa", "Governança, data owners, analistas de negócio e times técnicos."),
                ("Quando usar", "Ao definir um conceito, revisar nomenclatura ou documentar uma coluna e sua semântica."),
                ("Como usar", "Cadastrar ou revisar termos, associá-los a ativos e manter o dicionário alinhado ao catálogo real."),
                ("Decisões suportadas", "Padronização de significado, classificação semântica e aprovação de vocabulário corporativo."),
                ("Boas práticas", "Preferir termos claros, sem sinônimos soltos, e vincular o glossário ao que já existe no catálogo."),
                ("Erros comuns / cuidados", "Não criar termos sem owner ou sem uso prático; o glossário precisa refletir a operação real."),
                ("Contribuição para maturidade", "Eleva consistência conceitual e reduz ruído entre áreas de negócio e tecnologia."),
            ],
        ),
        screen(
            "Tags",
            ["/tags"],
            "Classificações e rótulos para enriquecer ativos, melhorar filtros e aumentar a leitura analítica do catálogo.",
            [
                ("O que é", "Tela de taxonomia e marcação dos ativos com labels de classificação corporativa."),
                ("Problema que resolve", "Facilita segmentação, busca e leitura de grupos de ativos por tema, criticidade ou uso."),
                ("Quem usa", "Governança, analistas, stewardship e liderança técnica."),
                ("Quando usar", "Ao organizar conjuntos de ativos, sinalizar classificação especial ou refinar filtros de navegação."),
                ("Como usar", "Cadastrar ou revisar tags e associá-las aos ativos que precisam ser agrupados de forma consistente."),
                ("Decisões suportadas", "Agrupamento analítico, filtros por tema e reforço da classificação sem depender só do nome técnico."),
                ("Boas práticas", "Padronizar nomenclatura e evitar explosão de tags sem governança; menos é mais quando há uso claro."),
                ("Erros comuns / cuidados", "Não usar tags como substituto de ownership, descrição ou certificação."),
                ("Contribuição para maturidade", "Melhora a organização do catálogo e aumenta a capacidade de priorização por tema ou domínio."),
            ],
        ),
        screen(
            "Data Quality e regras",
            ["/data-quality", "/data-quality/rules"],
            "Visão de qualidade, perfis, regras SQL e agendamentos amigáveis para manter o estado dos ativos sob controle.",
            [
                ("O que é", "Área de DQ com overview, histórico operacional, regras e automatização do perfilamento e das execuções."),
                ("Problema que resolve", "Torna visível o estado de completude, frescor, falhas e violações que podem comprometer o uso do ativo."),
                ("Quem usa", "Governança, times de dados, operação e stewardship."),
                ("Quando usar", "Ao definir verificações, acompanhar execução automática ou agir sobre falhas e degradações."),
                ("Como usar", "Revisar a visão geral, criar ou editar regras e observar última execução, próxima execução e status operacional."),
                ("Decisões suportadas", "Se um ativo está apto ao uso, se precisa revisão ou se deve entrar em tratamento prioritário."),
                ("Boas práticas", "Preferir regras claras e recorrentes, com labels amigáveis e configuração simples para o usuário final."),
                ("Erros comuns / cuidados", "Não tratar a tela como um catálogo de SQL solto; o foco é governança e operação com impacto mensurável."),
                ("Contribuição para maturidade", "Conecta qualidade ao uso real do catálogo e gera sinais objetivos para priorização.",
                ),
            ],
        ),
        screen(
            "Central de pendências",
            ["/governance/pending-center"],
            "Fila consolidada de solicitações e pendências de governança, com resumo, filtro e priorização operacional.",
            [
                ("O que é", "Central que reúne itens abertos, aprováveis, recorrentes e críticos em uma única fila consultável."),
                ("Problema que resolve", "Evita perder solicitações em áreas isoladas e dá visibilidade do volume real de governança pendente."),
                ("Quem usa", "Stewardship, data owners, líderes de governança e administradores do fluxo."),
                ("Quando usar", "Ao iniciar a rotina do dia, revisar backlog ou redistribuir prioridades entre aprovadores."),
                ("Como usar", "Filtrar a fila, interpretar os cartões-resumo e abrir cada pendência para decidir ou encaminhar."),
                ("Decisões suportadas", "Priorização de atendimento, distribuição de pendências e classificação por criticidade."),
                ("Boas práticas", "Tratar a fila como um mecanismo de decisão e não como lista estática; manter o fluxo atualizado diariamente."),
                ("Erros comuns / cuidados", "Não deixar a fila ser apenas um repositório de itens antigos sem dono ou sem SLA."),
                ("Contribuição para maturidade", "Torna a governança operacional, rastreável e mensurável."),
            ],
        ),
        screen(
            "Stewardship",
            ["/governance/stewardship"],
            "Fluxo de stewardship para solicitações, revisão e aprovação de ativos de dados.",
            [
                ("O que é", "Área de solicitação e aprovação de mudanças sobre ativos, owners, termos, certificação e revisões periódicas."),
                ("Problema que resolve", "Substitui decisões informais por um fluxo rastreável com contexto, comentários, responsáveis e histórico."),
                ("Quem usa", "Stewardship, data owners, administradores e usuários com permissão de leitura do fluxo."),
                ("Quando usar", "Ao pedir uma alteração relevante ou quando houver uma decisão de governança que precise de revisão formal."),
                ("Como usar", "Selecionar o ativo, preencher o tipo de solicitação, orientar a decisão e acompanhar o ciclo até aprovação ou rejeição."),
                ("Decisões suportadas", "Alteração de descrição, owner, termos, certificação e revisões periódicas de governança."),
                ("Boas práticas", "Abrir solicitações a partir do contexto do ativo sempre que possível e manter comentários objetivos e auditáveis."),
                ("Erros comuns / cuidados", "Não usar stewardship como tarefa burocrática sem decisão; o fluxo precisa terminar em ação concreta."),
                ("Contribuição para maturidade", "É o mecanismo de decisão e rastreabilidade que conecta catálogo, negócio e responsabilidade."),
            ],
        ),
        screen(
            "Certificação",
            ["/certification"],
            "Consolida o estado de certificação dos ativos e apoia revisões periódicas com visão clara de validade e confiança.",
            [
                ("O que é", "Tela de status de certificação, aprovações e validade dos ativos priorizados."),
                ("Problema que resolve", "Ajuda a separar ativos prontos para uso daqueles que ainda precisam de revisão ou reforço de governança."),
                ("Quem usa", "Governança, stewardship, data owners e liderança analítica."),
                ("Quando usar", "Ao revisar a confiança do catálogo e confirmar quais ativos estão formalmente aptos."),
                ("Como usar", "Abrir a lista, checar status, revisar itens pendentes e concluir ações de revalidação quando necessário."),
                ("Decisões suportadas", "Ativo certificado, ativo em revisão, ativo vencido ou ativo a priorizar para revalidação."),
                ("Boas práticas", "Manter critérios objetivos de certificação e revisar a validade com cadência realista para o negócio."),
                ("Erros comuns / cuidados", "Não confundir certificação com simples existência de metadados; a certificação exige decisão explícita."),
                ("Contribuição para maturidade", "Aumenta a confiança dos consumidores de dados e torna o catálogo operacionalmente mais útil."),
            ],
        ),
        screen(
            "Linhagem",
            ["/lineage"],
            "Visão de dependências, fluxos e relações entre fontes, tabelas e ativos do catálogo.",
            [
                ("O que é", "Camada visual e operacional da relação entre ativos, com origem, propagação e contexto de transformação."),
                ("Problema que resolve", "Permite entender impacto e dependências sem rastrear manualmente pipelines ou consultar múltiplos times."),
                ("Quem usa", "Arquitetura, engenharia de dados, stewardship e governança."),
                ("Quando usar", "Ao avaliar impacto de mudança, origem de um dado ou conexão entre ativos e pipelines."),
                ("Como usar", "Abrir o ativo, navegar pela linha do tempo/relacionamentos e validar a fonte automática de linhagem quando disponível."),
                ("Decisões suportadas", "Análise de impacto, análise de origem e revisão de dependência entre sistemas e tabelas."),
                ("Boas práticas", "Usar a linhagem como base para decisões de mudança e não apenas como visualização ilustrativa."),
                ("Erros comuns / cuidados", "Não assumir que a ausência de uma relação significa ausência de dependência; às vezes o conector ainda precisa ser calibrado."),
                ("Contribuição para maturidade", "Expõe dependências reais e fortalece o controle de impacto em produção."),
            ],
        ),
        screen(
            "Cockpit operacional",
            ["/ops/cockpit", "/ops/ingestion", "/scan-runs"],
            "Visão operacional de ingestão, correlação crítica e monitoramento dos sinais que afetam saúde e atualização.",
            [
                ("O que é", "Painel operacional que reúne ingestão, sinais críticos, correlação e referências de atualização dos ativos."),
                ("Problema que resolve", "Mostra o que está quebrando, o que está degradando e onde a operação precisa agir agora."),
                ("Quem usa", "Operação, engenharia, governança e liderança técnica."),
                ("Quando usar", "Durante o acompanhamento diário da plataforma ou em resposta a um incidente operacional."),
                ("Como usar", "Ler os blocos de ingestão e correlação, abrir detalhes e tratar prioridades por impacto real."),
                ("Decisões suportadas", "Priorização de falhas, correlação com DQ e incidentes e avaliação da urgência operacional."),
                ("Boas práticas", "Tratar o cockpit como fila de ação; se o sinal aparecer, ele deve virar investigação ou correção."),
                ("Erros comuns / cuidados", "Não usar o cockpit como relatório estático; sua utilidade está na ação rápida sobre a prioridade."),
                ("Contribuição para maturidade", "Integra sinais de operação e qualidade para fechar o ciclo de ação sobre o catálogo."),
            ],
        ),
        screen(
            "Incidentes e tickets",
            ["/incidents", "/incidents/tickets"],
            "Registro e acompanhamento de incidentes de dados, criticidade e encaminhamento operacional.",
            [
                ("O que é", "Área de gestão de incidentes e tickets com foco em criticidade, status e rastreabilidade do atendimento."),
                ("Problema que resolve", "Evita que falhas fiquem dispersas em conversas paralelas e cria um ponto único de acompanhamento."),
                ("Quem usa", "Operação, liderança de dados, stewardship e responsáveis técnicos."),
                ("Quando usar", "Quando houver falha de dados, incidente operacional, impacto no consumo ou necessidade de triagem."),
                ("Como usar", "Abrir o ticket, classificar criticidade, acompanhar andamento e registrar a resolução ou encaminhamento."),
                ("Decisões suportadas", "Aceite de incidente, priorização de tratamento e fechamento com rastreabilidade."),
                ("Boas práticas", "Registrar o contexto do impacto e manter o ticket ligado ao ativo ou processo afetado."),
                ("Erros comuns / cuidados", "Não encerrar sem entender a causa ou sem vínculo com o ativo afetado."),
                ("Contribuição para maturidade", "Converte falhas em processo rastreável e evita perda de contexto operacional."),
            ],
        ),
        screen(
            "Inbox",
            ["/inbox"],
            "Central de notificações e ações pendentes, com regras automáticas e responsabilidade por resposta.",
            [
                ("O que é", "Caixa de entrada operacional para receber alertas, pendências, aprovações e sinais de governança."),
                ("Problema que resolve", "Reúne em um único ponto o que exige atenção do usuário, evitando perda de notificações dispersas."),
                ("Quem usa", "Todos os perfis, com conteúdo e ações ajustadas ao papel de cada usuário."),
                ("Quando usar", "Ao iniciar o dia e ao revisar itens que precisam de ação direta ou leitura rápida."),
                ("Como usar", "Abrir as notificações, ler o contexto e executar a ação disponível ou encaminhar o item."),
                ("Decisões suportadas", "Aprovar, rejeitar, investigar, revisar ou apenas registrar leitura de um evento relevante."),
                ("Boas práticas", "Tratar a inbox como um fluxo de trabalho e não como armazenamento permanente de mensagens."),
                ("Erros comuns / cuidados", "Não deixar notificações críticas sem tratamento; isso reduz confiança e aumenta ruído."),
                ("Contribuição para maturidade", "Fecha o ciclo operacional entre detecção, notificação e ação humana."),
            ],
        ),
        screen(
            "Configuração",
            ["/admin/governance"],
            "Área de parâmetros de governança, retenção, visibilidade, pesos e regras operacionais da plataforma.",
            [
                ("O que é", "Tela administrativa para parametrizar políticas, retenção, compatibilidade, masking, stewardship e pesos de score."),
                ("Problema que resolve", "Centraliza configurações que sustentam o comportamento da plataforma sem espalhar regras em código."),
                ("Quem usa", "Admin para edição; editor para leitura; demais perfis sem acesso."),
                ("Quando usar", "Ao ajustar política operacional, revisar retenção, calibrar regras ou atualizar visibilidade."),
                ("Como usar", "Ler ou editar os blocos de configuração conforme o perfil; salvar apenas quando houver permissão de administração."),
                ("Decisões suportadas", "SLA, retenção, visibilidade, regras de stewardship, peso do score e legado da API."),
                ("Boas práticas", "Alterar parâmetros de forma consciente e documentada, com impacto operacional entendido antes da publicação."),
                ("Erros comuns / cuidados", "Não usar a área como ajuste pontual sem governança; cada mudança afeta a operação da plataforma."),
                ("Contribuição para maturidade", "Mantém a política do produto transparente, auditável e alinhada à governança corporativa."),
            ],
        ),
        screen(
            "Administração de usuários, papéis, permissões e aliases de busca",
            ["/admin/users", "/admin/roles", "/admin/permissions", "/search/aliases"],
            "Gestão de acesso e manutenção de rótulos e permissões do sistema.",
            [
                ("O que é", "Conjunto de telas administrativas para manter usuários, papéis, permissões e aliases de busca."),
                ("Problema que resolve", "Dá controle sobre quem acessa o quê e melhora a experiência de busca com sinônimos e termos alternativos."),
                ("Quem usa", "Administradores do sistema."),
                ("Quando usar", "Ao criar usuários, ajustar perfis, revisar permissões ou melhorar a recuperação de termos na busca."),
                ("Como usar", "Abrir a área administrativa correspondente, revisar cadastros e manter a governança de acesso e nomenclatura."),
                ("Decisões suportadas", "Acesso por perfil, concessão de permissões e padronização de aliases de pesquisa."),
                ("Boas práticas", "Preferir perfis enxutos, permissões explícitas e aliases realmente utilizados pelos usuários."),
                ("Erros comuns / cuidados", "Evitar permissões excessivas e aliases sem valor operacional, que poluem a recuperação dos resultados."),
                ("Contribuição para maturidade", "Sustenta RBAC, rastreabilidade e uma busca mais eficaz e humana."),
            ],
        ),
        screen(
            "Acesso sensível / Privacy Access",
            ["/privacy-access"],
            "Governança de acesso a informações sensíveis, com foco em política, exposição e controle.",
            [
                ("O que é", "Página para acompanhar e aplicar regras de acesso sensível e masking em ativos e contextos específicos."),
                ("Problema que resolve", "Evita exposição indevida e organiza a leitura de dados sensíveis por perfil e necessidade."),
                ("Quem usa", "Admin, governança, segurança da informação e data owners autorizados."),
                ("Quando usar", "Ao revisar exposições sensíveis, aplicar restrições ou validar regras de masking."),
                ("Como usar", "Abrir a tela, revisar os controles aplicados e ajustar o escopo somente quando houver autoridade para isso."),
                ("Decisões suportadas", "Bloqueio, masking, exceções e visão de conformidade por perfil."),
                ("Boas práticas", "Aplicar o princípio do menor privilégio e revisar regras sensíveis com registro de justificativa."),
                ("Erros comuns / cuidados", "Não usar a tela para expor dados reais a perfis não autorizados."),
                ("Contribuição para maturidade", "Protege o catálogo e reforça conformidade e governança de acesso."),
            ],
        ),
    ]

    for item in screens:
        elements.append(item)

    elements.extend(
        [
            {"type": "heading", "level": 1, "text": "Fluxo operacional recomendado"},
            bullets(
                [
                    "1. Inventariar a fonte e garantir a alimentação do catálogo por scans e ingestão.",
                    "2. Abrir o Explorer, completar descrição, owner, tags, termos e classificação dos ativos prioritários.",
                    "3. Registrar e acompanhar regras de Data Quality para os ativos mais críticos.",
                    "4. Usar Stewardship e Central de pendências para formalizar decisões, revisões e aprovações.",
                    "5. Validar certificação, lineage e cockpit operacional antes de considerar um ativo apto a consumo amplo.",
                    "6. Monitorar inbox, incidentes e auditoria para fechar o ciclo de detecção, tratamento e aprendizado.",
                ]
            ),
            {"type": "heading", "level": 1, "text": "Checklists de adoção e produção"},
            table(
                "Checklist de adoção",
                ["Item", "Condição de pronto"],
                [
                    ["Contas e perfis", "Usuários reais, perfis coerentes e RBAC validado."],
                    ["Inventário", "Fontes cadastradas e scans operando com regularidade."],
                    ["Ownership", "Ativos críticos com owner definido e rastreável."],
                    ["Glossário e tags", "Vocabulário e classificação aplicados aos ativos principais."],
                    ["DQ e certificação", "Regras e validações em funcionamento para os ativos críticos."],
                ],
                widths=[0.28, 0.72],
            ),
            table(
                "Checklist de produção",
                ["Item", "Critério"],
                [
                    ["Auditoria", "Mudanças importantes registradas e consultáveis."],
                    ["Inbox", "Notificações críticas chegam aos responsáveis corretos."],
                    ["Cockpit", "Sinais operacionais revisados diariamente."],
                    ["Configuração", "Parâmetros controlados e documentados."],
                    ["Retenção", "Histórico e arquivamento alinhados à política."],
                ],
                widths=[0.22, 0.78],
            ),
            {"type": "heading", "level": 1, "text": "Boas práticas para manter a área madura"},
            bullets(
                [
                    "Manter o catálogo vivo: documentação sem owner e sem atualização vira ruído.",
                    "Fechar ciclos: toda pendência deve terminar em aprovação, rejeição, correção ou justificativa registrada.",
                    "Priorizar o que move a operação: qualidade, certificação, lineage e ingestão devem orientar a rotina.",
                    "Tratar configuração como política e não como improviso técnico.",
                    "Usar o cockpit e a inbox como instrumentos de ação, não como páginas de leitura passiva.",
                    "Revisar a governança periodicamente para que o sistema continue aderente à operação real.",
                ]
            ),
            {"type": "heading", "level": 1, "text": "Conclusão executiva"},
            paragraph(
                "O t2c_data foi desenhado para transformar catálogo em rotina operacional de governança. Quando o produto é usado de forma"
                " disciplinada — inventário, ownership, DQ, certificação, linhagem, pendências e auditoria — a área de dados passa a operar"
                " com mais previsibilidade, menos improviso e mais capacidade de priorização."
            ),
        ]
    )

    return elements


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(cell.replace("\n", " ") for cell in row) + " |")
    return "\n".join(lines)


def render_markdown(elements: Sequence[object]) -> str:
    lines: list[str] = []
    for element in elements:
        if isinstance(element, dict) and element.get("type") == "cover":
            lines.append(f"# {element['title']}")
            lines.append("")
            lines.append(f"**{element['subtitle']}**")
            lines.append("")
            lines.append("**Metadados**")
            lines.append("")
            lines.append(markdown_table(["Campo", "Valor"], element["metadata"]))
            lines.append("")
            continue
        if isinstance(element, dict) and element.get("type") == "heading":
            level = int(element["level"])
            lines.append(f"{'#' * (level + 1)} {element['text']}")
            lines.append("")
            continue
        if isinstance(element, ParagraphBlock):
            lines.append(element.text)
            lines.append("")
            continue
        if isinstance(element, BulletBlock):
            for item in element.items:
                lines.append(f"- {item}")
            lines.append("")
            continue
        if isinstance(element, TableBlock):
            lines.append(f"**{element.title}**")
            lines.append("")
            lines.append(markdown_table(element.headers, element.rows))
            lines.append("")
            continue
        if isinstance(element, ScreenBlock):
            lines.append(f"## {element.title}")
            lines.append("")
            lines.append(f"**Rotas:** {', '.join(f'`{route}`' for route in element.routes)}")
            lines.append("")
            lines.append(element.summary)
            lines.append("")
            lines.append(markdown_table(["Campo", "Descrição"], element.rows))
            lines.append("")
            continue
    return "\n".join(lines).rstrip() + "\n"


def _docx_ns(tag: str) -> str:
    return f"{{http://schemas.openxmlformats.org/wordprocessingml/2006/main}}{tag}"


def _docx_text_paragraph(text: str, style: str | None = None, bold: bool = False, italic: bool = False, align: str | None = None) -> str:
    ppr = []
    if style:
        ppr.append(f'<w:pStyle w:val="{style}"/>')
    if align:
        ppr.append(f'<w:jc w:val="{align}"/>')
    run_props = []
    if bold:
        run_props.append("<w:b/>")
    if italic:
        run_props.append("<w:i/>")
    ppr_xml = f"<w:pPr>{''.join(ppr)}</w:pPr>" if ppr else ""
    rpr_xml = f"<w:rPr>{''.join(run_props)}</w:rPr>" if run_props else ""
    escaped = xml_escape(text)
    return f"<w:p>{ppr_xml}<w:r>{rpr_xml}<w:t xml:space='preserve'>{escaped}</w:t></w:r></w:p>"


def _docx_table(headers: Sequence[str], rows: Sequence[Sequence[str]], widths: Sequence[float] | None = None, style_name: str = "TableGrid") -> str:
    col_count = len(headers)
    if widths is None:
        widths = [1 / col_count for _ in headers]
    total_twips = 9000
    col_twips = [int(total_twips * ratio) for ratio in widths]
    grid = "".join(f'<w:gridCol w:w="{w}"/>' for w in col_twips)

    def cell(text: str, width: int, header: bool = False) -> str:
        bold = header
        style = "TableHeader" if header else "TableBody"
        return (
            "<w:tc>"
            "<w:tcPr>"
            f"<w:tcW w:w=\"{width}\" w:type=\"dxa\"/>"
            "</w:tcPr>"
            f"{_docx_text_paragraph(text, style=style, bold=bold)}"
            "</w:tc>"
        )

    header_row = "<w:tr>" + "".join(cell(h, width, header=True) for h, width in zip(headers, col_twips)) + "</w:tr>"
    body_rows = []
    for row in rows:
        body_rows.append("<w:tr>" + "".join(cell(str(value), width) for value, width in zip(row, col_twips)) + "</w:tr>")
    return (
        "<w:tbl>"
        "<w:tblPr>"
        f"<w:tblStyle w:val=\"{style_name}\"/>"
        "<w:tblW w:w=\"0\" w:type=\"auto\"/>"
        "</w:tblPr>"
        f"<w:tblGrid>{grid}</w:tblGrid>"
        f"{header_row}{''.join(body_rows)}"
        "</w:tbl>"
    )


def render_docx(elements: Sequence[object]) -> bytes:
    body_parts: list[str] = []
    body_parts.append(_docx_text_paragraph("t2c_data", style="Title", bold=True, align="center"))
    body_parts.append(_docx_text_paragraph("Manual corporativo de produto, operação e governança", style="Subtitle", italic=True, align="center"))
    body_parts.append(_docx_text_paragraph(" "))
    body_parts.append(_docx_table(["Campo", "Valor"], [list(row) for row in next(e for e in elements if isinstance(e, dict) and e.get("type") == "cover")["metadata"]], widths=[0.25, 0.75]))
    body_parts.append("<w:p><w:r><w:br w:type='page'/></w:r></w:p>")

    for element in elements:
        if isinstance(element, dict) and element.get("type") == "heading":
            level = int(element["level"])
            style = "Heading1" if level == 1 else "Heading2"
            body_parts.append(_docx_text_paragraph(element["text"], style=style, bold=True))
            continue
        if isinstance(element, ParagraphBlock):
            body_parts.append(_docx_text_paragraph(element.text, style="Body"))
            continue
        if isinstance(element, BulletBlock):
            for item in element.items:
                body_parts.append(_docx_text_paragraph(f"• {item}", style="Body"))
            continue
        if isinstance(element, TableBlock):
            body_parts.append(_docx_text_paragraph(element.title, style="Heading3", bold=True))
            body_parts.append(_docx_table(element.headers, element.rows, widths=element.widths))
            continue
        if isinstance(element, ScreenBlock):
            body_parts.append(_docx_text_paragraph(element.title, style="Heading2", bold=True))
            body_parts.append(_docx_text_paragraph(f"Rotas: {', '.join(element.routes)}", style="Body", italic=True))
            body_parts.append(_docx_text_paragraph(element.summary, style="Body"))
            body_parts.append(_docx_table(["Campo", "Descrição"], [[label, value] for label, value in element.rows], widths=[0.26, 0.74]))
            continue

    body_parts.append(_docx_text_paragraph(" ", style="Body"))
    body_xml = "".join(body_parts)
    document_xml = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main' "
        "xmlns:r='http://schemas.openxmlformats.org/officeDocument/2006/relationships'>"
        f"<w:body>{body_xml}<w:sectPr><w:pgSz w:w='11906' w:h='16838'/><w:pgMar w:top='1134' w:right='1134' w:bottom='1134' w:left='1134'/></w:sectPr></w:body>"
        "</w:document>"
    )

    styles_xml = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<w:styles xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'>"
        "<w:style w:type='paragraph' w:default='1' w:styleId='Normal'><w:name w:val='Normal'/><w:qFormat/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='22'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='Title'><w:name w:val='Title'/><w:basedOn w:val='Normal'/><w:uiPriority w:val='1'/><w:qFormat/><w:pPr><w:jc w:val='center'/></w:pPr><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='36'/><w:b/><w:color w:val='0F172A'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='Subtitle'><w:name w:val='Subtitle'/><w:basedOn w:val='Normal'/><w:pPr><w:jc w:val='center'/></w:pPr><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='20'/><w:color w:val='475569'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='Heading1'><w:name w:val='Heading1'/><w:basedOn w:val='Normal'/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='30'/><w:b/><w:color w:val='0F172A'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='Heading2'><w:name w:val='Heading2'/><w:basedOn w:val='Normal'/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='26'/><w:b/><w:color w:val='1E293B'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='Heading3'><w:name w:val='Heading3'/><w:basedOn w:val='Normal'/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='24'/><w:b/><w:color w:val='334155'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='Body'><w:name w:val='Body'/><w:basedOn w:val='Normal'/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='22'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='TableHeader'><w:name w:val='TableHeader'/><w:basedOn w:val='Body'/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='20'/><w:b/><w:color w:val='0F172A'/></w:rPr></w:style>"
        "<w:style w:type='paragraph' w:styleId='TableBody'><w:name w:val='TableBody'/><w:basedOn w:val='Body'/><w:rPr><w:rFonts w:ascii='Calibri' w:hAnsi='Calibri'/><w:sz w:val='20'/></w:rPr></w:style>"
        "</w:styles>"
    )

    content_types = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<Types xmlns='http://schemas.openxmlformats.org/package/2006/content-types'>"
        "<Default Extension='rels' ContentType='application/vnd.openxmlformats-package.relationships+xml'/>"
        "<Default Extension='xml' ContentType='application/xml'/>"
        "<Override PartName='/word/document.xml' ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml'/>"
        "<Override PartName='/word/styles.xml' ContentType='application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml'/>"
        "<Override PartName='/docProps/core.xml' ContentType='application/vnd.openxmlformats-package.core-properties+xml'/>"
        "<Override PartName='/docProps/app.xml' ContentType='application/vnd.openxmlformats-officedocument.extended-properties+xml'/>"
        "</Types>"
    )

    rels = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'>"
        "<Relationship Id='rId1' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument' Target='word/document.xml'/>"
        "</Relationships>"
    )
    doc_rels = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<Relationships xmlns='http://schemas.openxmlformats.org/package/2006/relationships'>"
        "<Relationship Id='rId1' Type='http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles' Target='styles.xml'/>"
        "</Relationships>"
    )
    utc_now = _dt.datetime.now(_dt.timezone.utc).isoformat()
    core_xml = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<cp:coreProperties xmlns:cp='http://schemas.openxmlformats.org/package/2006/metadata/core-properties' "
        "xmlns:dc='http://purl.org/dc/elements/1.1/' "
        "xmlns:dcterms='http://purl.org/dc/terms/' "
        "xmlns:dcmitype='http://purl.org/dc/dcmitype/' "
        "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>"
        "<dc:title>t2c_data Manual</dc:title>"
        "<dc:subject>Manual corporativo</dc:subject>"
        "<dc:creator>Codex</dc:creator>"
        "<cp:lastModifiedBy>Codex</cp:lastModifiedBy>"
        f"<dcterms:created xsi:type='dcterms:W3CDTF'>{utc_now}</dcterms:created>"
        f"<dcterms:modified xsi:type='dcterms:W3CDTF'>{utc_now}</dcterms:modified>"
        "</cp:coreProperties>"
    )
    app_xml = (
        "<?xml version='1.0' encoding='UTF-8' standalone='yes'?>"
        "<Properties xmlns='http://schemas.openxmlformats.org/officeDocument/2006/extended-properties' "
        "xmlns:vt='http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes'>"
        "<Application>Microsoft Office Word</Application>"
        "</Properties>"
    )

    buf = bytearray()
    with zipfile.ZipFile(Path("/tmp/t2c_manual_docx.zip"), "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", content_types)
        zf.writestr("_rels/.rels", rels)
        zf.writestr("docProps/core.xml", core_xml)
        zf.writestr("docProps/app.xml", app_xml)
        zf.writestr("word/document.xml", document_xml)
        zf.writestr("word/styles.xml", styles_xml)
        zf.writestr("word/_rels/document.xml.rels", doc_rels)
        for info in zf.infolist():
            pass
    data = Path("/tmp/t2c_manual_docx.zip").read_bytes()
    Path("/tmp/t2c_manual_docx.zip").unlink(missing_ok=True)
    return data


class PDFWriter:
    width = 595.28
    height = 841.89
    margin_x = 54
    margin_top = 54
    margin_bottom = 54

    def __init__(self) -> None:
        self.pages: list[list[str]] = []
        self.current_ops: list[str] = []
        self.y = self.height - self.margin_top
        self.page_number = 0
        self._new_page()

    def _new_page(self) -> None:
        if self.current_ops:
            self.pages.append(self.current_ops)
        self.current_ops = []
        self.y = self.height - self.margin_top
        self.page_number += 1

    def _ensure(self, needed: float) -> None:
        if self.y - needed < self.margin_bottom:
            self._new_page()

    @staticmethod
    def _escape(text: str) -> str:
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    @staticmethod
    def _encode(text: str) -> str:
        normalized = text.replace("—", "-").replace("–", "-")
        return PDFWriter._escape(normalized).encode("cp1252", errors="replace").decode("cp1252")

    @staticmethod
    def _estimate_lines(text: str, font_size: float, width: float, font_name: str = "Helvetica") -> list[str]:
        text = re.sub(r"\s+", " ", text.strip())
        if not text:
            return [""]
        avg_char = font_size * (0.53 if "Bold" not in font_name else 0.56)
        max_chars = max(12, int(width / avg_char))
        return textwrap.wrap(text, width=max_chars, break_long_words=False, break_on_hyphens=False) or [text]

    def _text(self, x: float, y: float, text: str, font: str, size: float, color: tuple[float, float, float] = (0, 0, 0)) -> None:
        self.current_ops.append(f"{color[0]:.3f} {color[1]:.3f} {color[2]:.3f} rg")
        self.current_ops.append(f"BT /{font} {size:.2f} Tf {x:.2f} {y:.2f} Td ({self._encode(text)}) Tj ET")

    def paragraph(self, text: str, size: float = 11, font: str = "Helvetica", leading: float | None = None, indent: float = 0, color: tuple[float, float, float] = (0.145, 0.161, 0.184), width: float | None = None) -> None:
        width = width or (self.width - 2 * self.margin_x - indent)
        lines = self._estimate_lines(text, size, width, font)
        leading = leading or size * 1.4
        self._ensure(len(lines) * leading + 8)
        x = self.margin_x + indent
        y = self.y
        for line in lines:
            self._text(x, y, line, font, size, color)
            y -= leading
        self.y = y - 2

    def heading(self, text: str, level: int = 1) -> None:
        if level == 1:
            size, font, color, spacing = 18, "Helvetica-Bold", (0.082, 0.11, 0.173), 16
        elif level == 2:
            size, font, color, spacing = 14.5, "Helvetica-Bold", (0.125, 0.17, 0.24), 12
        else:
            size, font, color, spacing = 12.5, "Helvetica-Bold", (0.22, 0.29, 0.38), 10
        self._ensure(size + spacing + 12)
        self.y -= 4
        self._text(self.margin_x, self.y, text, font, size, color)
        self.y -= spacing
        self.current_ops.append(f"{0.808:.3f} {0.84:.3f} {0.89:.3f} RG")
        self.current_ops.append(f"{self.margin_x:.2f} {self.y + 4:.2f} m {self.width - self.margin_x:.2f} {self.y + 4:.2f} l S")
        self.y -= 6

    def bullet_list(self, items: Sequence[str]) -> None:
        for item in items:
            self.paragraph(f"• {item}", size=10.7, indent=14, color=(0.145, 0.161, 0.184))

    def table(self, headers: Sequence[str], rows: Sequence[Sequence[str]], widths: Sequence[float] | None = None, title: str | None = None) -> None:
        if title:
            self.heading(title, level=3)
        if widths is None:
            widths = [1 / len(headers) for _ in headers]
        usable = self.width - 2 * self.margin_x
        col_widths = [usable * ratio for ratio in widths]
        header_font = "Helvetica-Bold"
        body_font = "Helvetica"
        header_size = 9
        body_size = 9.2
        line_pad = 6

        def row_height(cells: Sequence[str], fonts: Sequence[str], size: float) -> float:
            heights = []
            for idx, cell in enumerate(cells):
                lines = self._estimate_lines(cell, size, col_widths[idx] - 10, fonts[idx])
                heights.append(len(lines) * (size * 1.35) + line_pad)
            return max(22, max(heights))

        rows_all = [list(headers)] + [list(row) for row in rows]
        heights = [row_height(rows_all[0], [header_font] * len(headers), header_size)]
        for row in rows:
            heights.append(row_height(row, [body_font] * len(headers), body_size))
        total_needed = sum(heights) + 8
        self._ensure(total_needed)
        y = self.y

        def draw_row(cells: Sequence[str], header: bool, height: float) -> None:
            nonlocal y
            x = self.margin_x
            for idx, cell in enumerate(cells):
                w = col_widths[idx]
                fill = (0.96, 0.97, 0.98) if header or idx == 0 else (1, 1, 1)
                border = (0.82, 0.84, 0.89)
                self.current_ops.append(f"{fill[0]:.3f} {fill[1]:.3f} {fill[2]:.3f} rg")
                self.current_ops.append(f"{border[0]:.3f} {border[1]:.3f} {border[2]:.3f} RG")
                self.current_ops.append(f"{x:.2f} {y - height:.2f} {w:.2f} {height:.2f} re B")
                lines = self._estimate_lines(cell, header_size if header else body_size, w - 10, header_font if header else body_font)
                text_y = y - 14
                for line in lines:
                    self._text(x + 5, text_y, line, header_font if header else body_font, header_size if header else body_size, (0.082, 0.11, 0.173) if header else (0.145, 0.161, 0.184))
                    text_y -= (header_size if header else body_size) * 1.35
                x += w
            y -= height

        draw_row(headers, True, heights[0])
        for row, height in zip(rows, heights[1:]):
            draw_row(row, False, height)
        self.y = y - 10

    def add_page(self) -> None:
        self._new_page()

    def cover(self, title: str, subtitle: str, metadata: Sequence[tuple[str, str]]) -> None:
        self.current_ops.append("0.086 0.133 0.208 rg")
        self.current_ops.append(f"0 0 {self.width:.2f} {self.height:.2f} re f")
        self.current_ops.append("1 1 1 rg")
        self._text(self.margin_x, self.height - 140, title, "Helvetica-Bold", 30, (1, 1, 1))
        self._text(self.margin_x, self.height - 175, subtitle, "Helvetica", 14, (0.90, 0.93, 0.97))
        self.current_ops.append("0.965 0.972 0.98 rg")
        self.current_ops.append(f"{self.margin_x:.2f} {self.height - 260:.2f} {self.width - 2*self.margin_x:.2f} 146 re f")
        self.current_ops.append("0.80 0.84 0.90 RG")
        self.current_ops.append(f"{self.margin_x:.2f} {self.height - 260:.2f} {self.width - 2*self.margin_x:.2f} 146 re S")
        y = self.height - 292
        self._text(self.margin_x + 18, y + 86, "Metadados do documento", "Helvetica-Bold", 12, (0.082, 0.11, 0.173))
        for key, value in metadata:
            self._text(self.margin_x + 18, y, f"{key}: ", "Helvetica-Bold", 10.8, (0.145, 0.161, 0.184))
            self._text(self.margin_x + 120, y, value, "Helvetica", 10.8, (0.145, 0.161, 0.184))
            y -= 22
        self.y = 120
        self.current_ops.append("0.76 0.82 0.9 rg")
        self.current_ops.append(f"{self.margin_x:.2f} 86.00 {self.width - 2*self.margin_x:.2f} 1.5 re f")
        self.current_ops.append("0.145 0.161 0.184 rg")
        self._text(self.margin_x, 60, "t2c_data • manual corporativo oficial", "Helvetica", 10, (0.35, 0.42, 0.53))
        self.y = 100

    def render_elements(self, elements: Sequence[object]) -> None:
        cover = next(item for item in elements if isinstance(item, dict) and item.get("type") == "cover")
        self.cover(cover["title"], cover["subtitle"], cover["metadata"])
        self.add_page()
        for element in elements:
            if isinstance(element, dict) and element.get("type") == "heading":
                self.heading(element["text"], int(element["level"]))
                continue
            if isinstance(element, ParagraphBlock):
                self.paragraph(element.text)
                continue
            if isinstance(element, BulletBlock):
                self.bullet_list(element.items)
                continue
            if isinstance(element, TableBlock):
                self.table(element.headers, element.rows, widths=element.widths, title=element.title)
                continue
            if isinstance(element, ScreenBlock):
                self.heading(element.title, 2)
                self.paragraph(f"Rotas: {', '.join(element.routes)}", size=10, font="Helvetica-Oblique", color=(0.35, 0.42, 0.53))
                self.paragraph(element.summary, size=10.7, color=(0.145, 0.161, 0.184))
                self.table(["Campo", "Descrição"], [[label, value] for label, value in element.rows], widths=[0.23, 0.77])
                continue

    def build(self) -> bytes:
        pages = self.pages + [self.current_ops]
        objects: list[bytes] = []

        def obj(data: str | bytes) -> int:
            objects.append(data.encode("cp1252", errors="replace") if isinstance(data, str) else data)
            return len(objects)

        font_ids = {
            "F1": obj("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"),
            "F2": obj("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>"),
            "F3": obj("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Oblique >>"),
        }
        content_ids = []
        page_ids = []
        for index, ops in enumerate(pages, start=1):
            footer = f"BT /F1 8 Tf {self.margin_x:.2f} 28.00 Td (Página {index}) Tj ET"
            stream = "\n".join(ops + [footer])
            content_ids.append(obj(f"<< /Length {len(stream.encode('cp1252', errors='replace'))} >>\nstream\n{stream}\nendstream"))
        pages_obj_id = len(objects) + len(content_ids) + 1
        kids = []
        for content_id in content_ids:
            page_obj = f"<< /Type /Page /Parent {pages_obj_id} 0 R /MediaBox [0 0 {self.width:.2f} {self.height:.2f}] /Resources << /Font << /F1 {font_ids['F1']} 0 R /F2 {font_ids['F2']} 0 R /F3 {font_ids['F3']} 0 R >> >> /Contents {content_id} 0 R >>"
            page_ids.append(obj(page_obj))
            kids.append(f"{page_ids[-1]} 0 R")
        pages_obj = obj(f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(page_ids)} >>")
        catalog_obj = obj(f"<< /Type /Catalog /Pages {pages_obj} 0 R >>")
        info_obj = obj("<< /Title (t2c_data Manual) /Author (Codex) /Subject (Manual corporativo) >>")

        xref_positions = []
        result = bytearray(b"%PDF-1.4\n")
        for idx, data in enumerate(objects, start=1):
            xref_positions.append(len(result))
            result.extend(f"{idx} 0 obj\n".encode("latin-1"))
            result.extend(data)
            result.extend(b"\nendobj\n")
        start_xref = len(result)
        result.extend(f"xref\n0 {len(objects)+1}\n".encode("latin-1"))
        result.extend(b"0000000000 65535 f \n")
        for pos in xref_positions:
            result.extend(f"{pos:010d} 00000 n \n".encode("latin-1"))
        result.extend(
            f"trailer << /Size {len(objects)+1} /Root {catalog_obj} 0 R /Info {info_obj} 0 R >>\nstartxref\n{start_xref}\n%%EOF".encode(
                "latin-1"
            )
        )
        return bytes(result)


def build_profile_rows() -> list[list[str]]:
    return [
        ["Admin", "Controle total da plataforma", "Visualização, edição, aprovação e configuração."],
        ["Editor", "Operação funcional", "Leitura ampla e execução operacional, sem áreas administrativas."],
        ["Visualizador", "Consulta e leitura", "Acompanhamento sem ações de mutação."],
        ["Stewardship", "Revisão e aprovação", "Abertura e aprovação de solicitações de stewardship."],
        ["Data owner", "Responsável de dados", "Leitura ampla e participação no fluxo de stewardship."],
    ]


def build_navigation_rows() -> list[list[str]]:
    return [
        ["Home / executivo", "/, /dashboard", "Leitura de maturidade, cobertura e priorização."],
        ["Busca e exploração", "/search, /explorer, /tables/[id]", "Descoberta de ativos e detalhe técnico."],
        ["Governança", "/governance, /certification, /privacy-access, /data-owners, /glossary, /tags", "Ownership, vocabulário, classificação e decisão."],
        ["Qualidade", "/data-quality, /data-quality/rules", "Regras, perfis e execução de DQ."],
        ["Operação", "/ops/cockpit, /ops/ingestion, /datasources, /scan-runs", "Ingestão, sinais operacionais e saúde das fontes."],
        ["Fluxo de decisão", "/governance/pending-center, /governance/stewardship, /inbox", "Pendências, aprovações e notificações."],
        ["Administração", "/admin/governance, /admin/users, /admin/roles, /admin/permissions, /search/aliases", "Configuração, RBAC e manutenção operacional."],
        ["Observabilidade", "/incidents, /audit", "Incidentes, rastreabilidade e apoio à análise."],
    ]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    elements = build_document_elements()
    markdown = render_markdown(elements)
    MD_PATH.write_text(markdown, encoding="utf-8")

    docx_bytes = render_docx(elements)
    DOCX_PATH.write_bytes(docx_bytes)

    pdf = PDFWriter()
    pdf.render_elements(elements)
    PDF_PATH.write_bytes(pdf.build())

    print(f"Generated: {MD_PATH}")
    print(f"Generated: {DOCX_PATH}")
    print(f"Generated: {PDF_PATH}")


if __name__ == "__main__":
    main()
