"""
Clause definitions for prenuptial and divorce documents.
"""
from clauses import Clause, ClauseRegistry, always_include, has_property_separation, has_alimony, has_children, is_prenuptial, is_divorce
from models import DocumentInput
from template_processor import TemplateProcessor
from signature_block import SignatureBlockGenerator
from typing import List


def create_prenuptial_clauses() -> ClauseRegistry:
    """Create clause registry for prenuptial agreements."""
    registry = ClauseRegistry()
    
    # Header clause
    registry.register(Clause(
        name="prenuptial_header",
        condition=lambda d: is_prenuptial(d),
        content=lambda d: f"""PRENUPTIAL AGREEMENT

This Prenuptial Agreement ("Agreement") is entered into on {TemplateProcessor.format_date(d.custom_values.get("marriage_date", "TBD"))}, 
by and between:

{d.parties[0].name}
{d.parties[0].address}
(hereinafter referred to as "Party A" or the "{d.parties[0].role.capitalize()}")

and

{d.parties[1].name}
{d.parties[1].address}
(hereinafter referred to as "Party B" or the "{d.parties[1].role.capitalize()}")


WHEREAS, the parties intend to be married; and

WHEREAS, the parties desire to define their respective rights and obligations 
regarding property, assets, and financial matters;

NOW, THEREFORE, in consideration of the mutual promises contained herein, 
the parties agree as follows:"""
    ))
    
    # Definitions clause
    registry.register(Clause(
        name="definitions",
        condition=lambda d: is_prenuptial(d),
        content=lambda d: """1. DEFINITIONS

1.1 "Separate Property" means all property owned by either party before the marriage, 
    and all property acquired by either party during the marriage by gift, inheritance, 
    or as separate property as defined by law.

1.2 "Marital Property" means all property acquired during the marriage, except as 
    otherwise defined as Separate Property."""
    ))
    
    # Property separation clause
    registry.register(Clause(
        name="property_separation_clause",
        condition=lambda d: is_prenuptial(d) and has_property_separation(d),
        content=lambda d: f"""2. PROPERTY RIGHTS

2.1 Each party shall retain sole ownership and control of their Separate Property, 
    free from any claim by the other party.

2.2 Each party waives any right, title, or interest in the Separate Property of 
    the other party.

2.3 Marital Property shall be divided according to the laws of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}, 
    unless otherwise specified in this Agreement."""
    ))
    
    # Default property clause (when property_separation is False)
    registry.register(Clause(
        name="default_property_clause",
        condition=lambda d: is_prenuptial(d) and not has_property_separation(d),
        content=lambda d: f"""2. PROPERTY RIGHTS

2.1 Property acquired during the marriage shall be considered Marital Property 
    and subject to division according to the laws of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}."""
    ))
    
    # Alimony waiver clause
    registry.register(Clause(
        name="alimony_waiver_clause",
        condition=lambda d: is_prenuptial(d) and has_alimony(d),
        content=lambda d: """3. SPOUSAL SUPPORT

3.1 In the event of separation or divorce, neither party shall be entitled to 
    spousal support or alimony from the other party.

3.2 Each party waives any right to seek spousal support, maintenance, or alimony, 
    except as may be required by law."""
    ))
    
    # Children clause
    registry.register(Clause(
        name="children_clause",
        condition=lambda d: is_prenuptial(d) and has_children(d),
        content=lambda d: """4. CHILDREN

4.1 This Agreement does not affect the rights and obligations of the parties 
    regarding children, which shall be determined according to the best interests 
    of the children and applicable law.

4.2 Child support obligations, if any, shall be determined separately and are 
    not affected by this Agreement."""
    ))
    
    # Governing law clause
    registry.register(Clause(
        name="governing_law_clause",
        condition=lambda d: is_prenuptial(d),
        content=lambda d: f"""5. GOVERNING LAW

This Agreement shall be governed by and construed in accordance with the laws 
of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}."""
    ))
    
    # Entire agreement clause
    registry.register(Clause(
        name="entire_agreement_clause",
        condition=lambda d: is_prenuptial(d),
        content=lambda d: """6. ENTIRE AGREEMENT

This Agreement constitutes the entire agreement between the parties and supersedes 
all prior agreements, understandings, or representations."""
    ))
    
    return registry


def create_divorce_clauses() -> ClauseRegistry:
    """Create clause registry for divorce settlement agreements."""
    registry = ClauseRegistry()
    
    # Header clause
    registry.register(Clause(
        name="divorce_header",
        condition=lambda d: is_divorce(d),
        content=lambda d: f"""DIVORCE SETTLEMENT AGREEMENT

This Divorce Settlement Agreement ("Agreement") is entered into on 
{TemplateProcessor.format_date(d.custom_values.get("divorce_date", "TBD"))}, by and between:

{d.parties[0].name}
{d.parties[0].address}
(hereinafter referred to as "Party A" or the "{d.parties[0].role.capitalize()}")

and

{d.parties[1].name}
{d.parties[1].address}
(hereinafter referred to as "Party B" or the "{d.parties[1].role.capitalize()}")


WHEREAS, the parties were married on {TemplateProcessor.format_date(d.custom_values.get("marriage_date", "TBD"))}; and

WHEREAS, the parties have decided to dissolve their marriage and have reached 
an agreement regarding the division of property, assets, and other matters;

NOW, THEREFORE, in consideration of the mutual promises contained herein, 
the parties agree as follows:"""
    ))
    
    # Dissolution clause
    registry.register(Clause(
        name="dissolution_clause",
        condition=lambda d: is_divorce(d),
        content=lambda d: f"""1. DISSOLUTION OF MARRIAGE

1.1 The parties agree to dissolve their marriage and seek a divorce decree 
    from the appropriate court in {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}.

1.2 Both parties agree that irreconcilable differences have led to the 
    breakdown of the marriage."""
    ))
    
    # Property separation clause
    registry.register(Clause(
        name="divorce_property_separation_clause",
        condition=lambda d: is_divorce(d) and has_property_separation(d),
        content=lambda d: """2. PROPERTY DIVISION

2.1 Each party shall retain sole ownership of their separate property, 
    including property owned before the marriage and property acquired by 
    gift or inheritance during the marriage.

2.2 Marital property shall be divided as follows:
    - Each party shall receive 50% of the value of all marital assets
    - Each party shall be responsible for 50% of all marital debts
    - The parties agree to cooperate in the valuation and division of assets

2.3 Each party waives any further claim to the property of the other party, 
    except as specifically provided in this Agreement."""
    ))
    
    # Default property clause
    registry.register(Clause(
        name="divorce_default_property_clause",
        condition=lambda d: is_divorce(d) and not has_property_separation(d),
        content=lambda d: f"""2. PROPERTY DIVISION

2.1 The parties agree to divide all marital property according to the laws 
    of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")} and as mutually agreed upon.

2.2 The parties shall cooperate in identifying, valuing, and dividing all 
    marital assets and debts."""
    ))
    
    # Alimony waiver clause
    registry.register(Clause(
        name="divorce_alimony_waiver_clause",
        condition=lambda d: is_divorce(d) and has_alimony(d),
        content=lambda d: """3. SPOUSAL SUPPORT / ALIMONY

3.1 The parties agree that no spousal support or alimony shall be paid by 
    either party to the other.

3.2 Each party waives any right to seek spousal support, maintenance, or 
    alimony, now or in the future, except as may be required by law."""
    ))
    
    # Alimony provision clause
    registry.register(Clause(
        name="divorce_alimony_provision_clause",
        condition=lambda d: is_divorce(d) and not has_alimony(d),
        content=lambda d: f"""3. SPOUSAL SUPPORT / ALIMONY

3.1 The parties agree to the following spousal support arrangement:
    - [To be determined based on specific circumstances]
    - Support shall be reviewed and may be modified based on changed circumstances

3.2 Any spousal support obligations shall be determined according to the 
    laws of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}."""
    ))
    
    # Children clause
    registry.register(Clause(
        name="divorce_children_clause",
        condition=lambda d: is_divorce(d) and has_children(d),
        content=lambda d: f"""4. CHILDREN

4.1 CUSTODY: The parties agree to [joint/shared] custody of the children, 
    with the following arrangement:
    - [Custody arrangement details to be specified]

4.2 CHILD SUPPORT: The parties agree that child support shall be determined 
    according to the child support guidelines of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}.

4.3 VISITATION: The non-custodial parent shall have reasonable visitation 
    rights as agreed upon by the parties or as ordered by the court.

4.4 The parties agree that all decisions regarding the children shall be 
    made in the best interests of the children."""
    ))
    
    # Other provisions clause
    registry.register(Clause(
        name="other_provisions_clause",
        condition=lambda d: is_divorce(d),
        content=lambda d: """5. OTHER PROVISIONS

5.1 Each party represents that they have had the opportunity to consult with 
    independent legal counsel regarding this Agreement.

5.2 Each party acknowledges that they have read and understand this Agreement 
    and are entering into it voluntarily.

5.3 This Agreement may be modified only by a written agreement signed by both parties."""
    ))
    
    # Governing law clause
    registry.register(Clause(
        name="divorce_governing_law_clause",
        condition=lambda d: is_divorce(d),
        content=lambda d: f"""6. GOVERNING LAW

This Agreement shall be governed by and construed in accordance with the laws 
of {d.custom_values.get("jurisdiction", "the applicable jurisdiction")}."""
    ))
    
    # Entire agreement clause
    registry.register(Clause(
        name="divorce_entire_agreement_clause",
        condition=lambda d: is_divorce(d),
        content=lambda d: """7. ENTIRE AGREEMENT

This Agreement constitutes the entire agreement between the parties regarding 
the dissolution of their marriage and supersedes all prior agreements."""
    ))
    
    return registry


def get_clause_order(document_type: str) -> List[str]:
    """Get the order of clauses for a document type."""
    if document_type == "prenuptial":
        return [
            "prenuptial_header",
            "definitions",
            "property_separation_clause",
            "default_property_clause",
            "alimony_waiver_clause",
            "children_clause",
            "governing_law_clause",
            "entire_agreement_clause"
        ]
    elif document_type == "divorce":
        return [
            "divorce_header",
            "dissolution_clause",
            "divorce_property_separation_clause",
            "divorce_default_property_clause",
            "divorce_alimony_waiver_clause",
            "divorce_alimony_provision_clause",
            "divorce_children_clause",
            "other_provisions_clause",
            "divorce_governing_law_clause",
            "divorce_entire_agreement_clause"
        ]
    else:
        return []

