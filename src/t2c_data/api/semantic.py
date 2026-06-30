from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from t2c_data.core.db import get_db
from t2c_data.core.deps import require_roles
from t2c_data.models.auth import User
from t2c_data.models.semantic import SemanticDataProduct, SemanticDomain
from t2c_data.schemas.semantic import (
    SemanticDomainCreate,
    SemanticDomainDetailOut,
    SemanticDomainOut,
    SemanticDomainPageOut,
    SemanticDomainSuggestionOut,
    SemanticDomainUpdate,
    SemanticLinkCreate,
    SemanticLinkOut,
    SemanticProductCreate,
    SemanticProductDetailOut,
    SemanticProductOut,
    SemanticProductPageOut,
    SemanticProductUpdate,
)
from t2c_data.features.pagination import paginate_items
from t2c_data.features.semantic.service import (
    add_domain_link,
    add_product_link,
    create_domain,
    create_product,
    delete_domain,
    delete_link,
    delete_product,
    find_product_detail_for_table,
    list_domain_detail,
    list_domain_links,
    list_domain_suggestions,
    list_domains,
    get_product_summary,
    list_product_links,
    list_product_detail,
    list_products,
    update_domain,
    update_product,
)

router = APIRouter(prefix="/semantic", tags=["semantic"])


@router.get("/domains", response_model=SemanticDomainPageOut)
def semantic_domains_list(
    q: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SemanticDomainPageOut:
    payload = list_domains(db, q=q, page=page, page_size=page_size)
    return SemanticDomainPageOut(**payload.model_dump(), suggestions=list_domain_suggestions(db))


@router.get("/domains/suggestions", response_model=list[SemanticDomainSuggestionOut])
def semantic_domain_suggestions(
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[SemanticDomainSuggestionOut]:
    return list_domain_suggestions(db)


@router.post("/domains", response_model=SemanticDomainOut, status_code=status.HTTP_201_CREATED)
def semantic_domains_create(
    payload: SemanticDomainCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> SemanticDomainOut:
    domain = create_domain(db, payload)
    db.commit()
    db.refresh(domain)
    return SemanticDomainOut.model_validate(domain)


@router.get("/domains/{domain_slug}", response_model=SemanticDomainDetailOut)
def semantic_domains_get(
    domain_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SemanticDomainDetailOut:
    return list_domain_detail(db, slug=domain_slug)


@router.patch("/domains/{domain_slug}", response_model=SemanticDomainOut)
def semantic_domains_patch(
    domain_slug: str,
    payload: SemanticDomainUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> SemanticDomainOut:
    domain = db.scalar(select(SemanticDomain).where(SemanticDomain.slug == domain_slug))
    if not domain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
    updated = update_domain(db, domain, payload)
    db.commit()
    db.refresh(updated)
    return SemanticDomainOut.model_validate(updated)


@router.delete("/domains/{domain_slug}", status_code=status.HTTP_204_NO_CONTENT)
def semantic_domains_delete(
    domain_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> None:
    domain = db.scalar(select(SemanticDomain).where(SemanticDomain.slug == domain_slug))
    if not domain:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Domain not found")
    delete_domain(db, domain)
    db.commit()
    return None


@router.get("/domains/{domain_slug}/links", response_model=list[SemanticLinkOut])
def semantic_domains_links_list(
    domain_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[SemanticLinkOut]:
    return list_domain_links(db, domain_slug=domain_slug)


@router.post("/domains/{domain_slug}/links", response_model=SemanticLinkOut, status_code=status.HTTP_201_CREATED)
def semantic_domains_links_create(
    domain_slug: str,
    payload: SemanticLinkCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> SemanticLinkOut:
    link = add_domain_link(db, domain_slug=domain_slug, payload=payload)
    db.commit()
    db.refresh(link)
    return SemanticLinkOut.model_validate(link)


@router.delete("/domains/{domain_slug}/links/{link_id}", status_code=status.HTTP_204_NO_CONTENT)
def semantic_domains_links_delete(
    domain_slug: str,
    link_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> None:
    delete_link(db, link_id)
    db.commit()
    return None


@router.get("/data-products", response_model=SemanticProductPageOut)
def semantic_products_list(
    q: str | None = Query(default=None),
    domain_slug: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=25, ge=1, le=100),
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SemanticProductPageOut:
    return list_products(db, q=q, domain_slug=domain_slug, page=page, page_size=page_size)


@router.post("/data-products", response_model=SemanticProductOut, status_code=status.HTTP_201_CREATED)
def semantic_products_create(
    payload: SemanticProductCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> SemanticProductOut:
    product = create_product(db, payload)
    db.commit()
    db.refresh(product)
    return list_product_detail(db, slug=product.slug, include_domain=True)


@router.get("/data-products/for-table/{table_id}", response_model=SemanticProductDetailOut | None)
def semantic_products_for_table(
    table_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SemanticProductDetailOut | None:
    return find_product_detail_for_table(db, table_id=table_id, current_user=current_user)


@router.get("/data-products/{product_slug}", response_model=SemanticProductDetailOut)
def semantic_products_get(
    product_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> SemanticProductDetailOut:
    return list_product_detail(db, slug=product_slug, include_domain=True)


@router.get("/data-products/{product_slug}/summary", response_model=dict[str, object])
def semantic_products_summary(
    product_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> dict[str, object]:
    return get_product_summary(db, slug=product_slug)


@router.patch("/data-products/{product_slug}", response_model=SemanticProductOut)
def semantic_products_patch(
    product_slug: str,
    payload: SemanticProductUpdate,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> SemanticProductOut:
    product = db.scalar(select(SemanticDataProduct).where(SemanticDataProduct.slug == product_slug))
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
    updated = update_product(db, product, payload)
    db.commit()
    db.refresh(updated)
    return list_product_detail(db, slug=updated.slug, include_domain=True)


@router.delete("/data-products/{product_slug}", status_code=status.HTTP_204_NO_CONTENT)
def semantic_products_delete(
    product_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> None:
    product = db.scalar(select(SemanticDataProduct).where(SemanticDataProduct.slug == product_slug))
    if not product:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data product not found")
    delete_product(db, product)
    db.commit()
    return None


@router.get("/data-products/{product_slug}/links", response_model=list[SemanticLinkOut])
def semantic_products_links_list(
    product_slug: str,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor", "viewer")),
) -> list[SemanticLinkOut]:
    return list_product_links(db, product_slug=product_slug)


@router.post("/data-products/{product_slug}/links", response_model=SemanticLinkOut, status_code=status.HTTP_201_CREATED)
def semantic_products_links_create(
    product_slug: str,
    payload: SemanticLinkCreate,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> SemanticLinkOut:
    link = add_product_link(db, product_slug=product_slug, payload=payload)
    db.commit()
    db.refresh(link)
    return SemanticLinkOut.model_validate(link)


@router.delete("/data-products/{product_slug}/links/{link_id}", status_code=status.HTTP_204_NO_CONTENT)
def semantic_products_links_delete(
    product_slug: str,
    link_id: int,
    db: Session = Depends(get_db),
    _: User = Depends(require_roles("admin", "editor")),
) -> None:
    delete_link(db, link_id)
    db.commit()
    return None
