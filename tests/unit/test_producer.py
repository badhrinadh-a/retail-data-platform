"""
Unit tests for the Kafka producer data generators.

Tests schema correctness, value ranges, and type contracts for all
generated entities (customers, orders, inventory, payments).
"""

import time
import uuid

from src.kafka.producer import (
    TOPICS,
    generate_customer,
    generate_inventory,
    generate_order,
    generate_payment,
)

# =============================================================================
# Test: Customer Generation
# =============================================================================


class TestGenerateCustomer:
    def test_customer_has_required_fields(self):
        """Customer must contain all expected keys."""
        customer = generate_customer()
        required_keys = {
            "customer_id",
            "name",
            "email",
            "address",
            "created_at",
        }
        assert required_keys == set(customer.keys())

    def test_customer_id_is_valid_uuid(self):
        """customer_id must be a valid UUID4."""
        customer = generate_customer()
        parsed = uuid.UUID(customer["customer_id"], version=4)
        assert str(parsed) == customer["customer_id"]

    def test_customer_email_format(self):
        """email must contain @ symbol."""
        customer = generate_customer()
        assert "@" in customer["email"]

    def test_customer_name_is_nonempty_string(self):
        """name must be a non-empty string."""
        customer = generate_customer()
        assert isinstance(customer["name"], str)
        assert len(customer["name"]) > 0

    def test_customer_created_at_is_recent_timestamp(self):
        """created_at should be a Unix timestamp close to now."""
        before = time.time()
        customer = generate_customer()
        after = time.time()
        assert before <= customer["created_at"] <= after


# =============================================================================
# Test: Order Generation
# =============================================================================


class TestGenerateOrder:
    def test_order_has_required_fields(self):
        """Order must contain all expected keys."""
        order = generate_order("test-customer-id")
        required_keys = {
            "order_id",
            "customer_id",
            "amount",
            "status",
            "created_at",
        }
        assert required_keys == set(order.keys())

    def test_order_references_correct_customer(self):
        """order.customer_id must match the provided customer_id."""
        order = generate_order("cust-123")
        assert order["customer_id"] == "cust-123"

    def test_order_id_is_valid_uuid(self):
        """order_id must be a valid UUID4."""
        order = generate_order("cust-123")
        parsed = uuid.UUID(order["order_id"], version=4)
        assert str(parsed) == order["order_id"]

    def test_order_amount_is_positive(self):
        """amount must be > 0."""
        for _ in range(50):  # statistical test over multiple runs
            order = generate_order("cust-123")
            assert order["amount"] > 0

    def test_order_amount_range(self):
        """amount must be between 10.0 and 500.0."""
        for _ in range(50):
            order = generate_order("cust-123")
            assert 10.0 <= order["amount"] <= 500.0

    def test_order_amount_has_two_decimals(self):
        """amount should be rounded to 2 decimal places."""
        order = generate_order("cust-123")
        amount_str = str(order["amount"])
        if "." in amount_str:
            decimals = len(amount_str.split(".")[1])
            assert decimals <= 2

    def test_order_status_is_valid(self):
        """status must be one of the allowed values."""
        valid_statuses = {"PENDING", "COMPLETED", "CANCELLED"}
        for _ in range(50):
            order = generate_order("cust-123")
            assert order["status"] in valid_statuses


# =============================================================================
# Test: Inventory Generation
# =============================================================================


class TestGenerateInventory:
    def test_inventory_has_required_fields(self):
        """Inventory record must contain all expected keys."""
        inventory = generate_inventory()
        required_keys = {
            "item_id",
            "sku",
            "quantity",
            "location",
            "updated_at",
        }
        assert required_keys == set(inventory.keys())

    def test_inventory_item_id_is_valid_uuid(self):
        """item_id must be a valid UUID4."""
        inventory = generate_inventory()
        parsed = uuid.UUID(inventory["item_id"], version=4)
        assert str(parsed) == inventory["item_id"]

    def test_inventory_sku_is_13_digits(self):
        """SKU (EAN-13) must be exactly 13 characters."""
        inventory = generate_inventory()
        assert len(inventory["sku"]) == 13
        assert inventory["sku"].isdigit()

    def test_inventory_quantity_is_non_negative(self):
        """quantity must be >= 0."""
        for _ in range(50):
            inventory = generate_inventory()
            assert inventory["quantity"] >= 0

    def test_inventory_quantity_range(self):
        """quantity must be between 0 and 100."""
        for _ in range(50):
            inventory = generate_inventory()
            assert 0 <= inventory["quantity"] <= 100


# =============================================================================
# Test: Payment Generation
# =============================================================================


class TestGeneratePayment:
    def test_payment_has_required_fields(self):
        """Payment must contain all expected keys."""
        payment = generate_payment("test-order-id")
        required_keys = {
            "payment_id",
            "order_id",
            "method",
            "status",
            "processed_at",
        }
        assert required_keys == set(payment.keys())

    def test_payment_references_correct_order(self):
        """payment.order_id must match the provided order_id."""
        payment = generate_payment("ord-456")
        assert payment["order_id"] == "ord-456"

    def test_payment_id_is_valid_uuid(self):
        """payment_id must be a valid UUID4."""
        payment = generate_payment("ord-456")
        parsed = uuid.UUID(payment["payment_id"], version=4)
        assert str(parsed) == payment["payment_id"]

    def test_payment_method_is_valid(self):
        """method must be one of the allowed payment methods."""
        valid_methods = {"CREDIT_CARD", "PAYPAL", "BITCOIN"}
        for _ in range(50):
            payment = generate_payment("ord-456")
            assert payment["method"] in valid_methods

    def test_payment_status_is_valid(self):
        """status must be SUCCESS or FAILED."""
        valid_statuses = {"SUCCESS", "FAILED"}
        for _ in range(50):
            payment = generate_payment("ord-456")
            assert payment["status"] in valid_statuses


# =============================================================================
# Test: Topic Configuration
# =============================================================================


class TestTopicConfig:
    def test_all_topics_defined(self):
        """TOPICS dict must contain all four event types."""
        assert set(TOPICS.keys()) == {
            "orders",
            "customers",
            "inventory",
            "payments",
        }

    def test_topic_values_are_strings(self):
        """Topic names must be non-empty strings."""
        for key, value in TOPICS.items():
            assert isinstance(value, str)
            assert len(value) > 0
