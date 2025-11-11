""" Module for storing shared dataclasses """

from dataclasses import dataclass
from typing import Optional


@dataclass
class KeypayReports:
    """ Stores whole reports provided Keypay """
    payroll: list[dict[str, dict | float]]
    qtd_payroll: Optional[list[dict[str, dict | float]]]
    mtd_payroll: Optional[list[dict[str, dict | float]]]
    leave_liability: list[dict]
    payg_withholding: list[dict]


@dataclass
class PayrollRecReports:
    """ Stores the simplified balance sheet and general ledger reports """
    ledger_category_key: Optional[dict]
    ledger_data_key: Optional[dict]
    balance_category_key: Optional[dict]
    balance_data_key: Optional[dict]
    balance_sheet: Optional[list[list[str]]]

    def __init__(self):
        self.ledger_category_key = None
        self.ledger_data_key = None
        self.balance_category_key = None
        self.balance_data_key = None
        self.balance_sheet = None
