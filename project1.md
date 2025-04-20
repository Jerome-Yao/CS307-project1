# E-R diagram
![[E-R.png]]
- The E-R diagram is drawn on the processon

# Database Design
![[database-diagram 2.png]]

### **1. supply_center**
- ​**Purpose**: Stores information about supply centers.
- ​**Columns**:
    - `center_name` (Primary Key): Unique name of the supply center.
    - `director`: Name of the director managing the center.
### ​**2. `client`**
- ​**Purpose**: Stores client details and their associated supply centers.
- ​**Columns**:
    - `client_id` (PK): Auto-incremented unique identifier for the client.
    - `client_name`: Unique name of the client.
    - `country`, `city`, `industry`: Geographic and business details of the client.
    - `supply_center` (Foreign Key): Links the client to their assigned supply center, which is defined by clients country.
### ​**3. `contract`**
- ​**Purpose**: Stores contracts basic info.
- ​**Columns**:
    - `contract_number` (PK): Unique identifier for the contract.
    - `client_name` (Foreign Key): References the client who signed the contract.
    - `contract_date`: Date when the contract was signed.
### ​**4. `sales`**
- ​**Purpose**: Stores salesman details.
- ​**Columns**:
    - `salesman_number` (PK): Unique identifier for the salesman.
    - `salesman_name`: Name of the salesperson.
    - `gender`, `age`, `mobile_number`: Basic information of the salesman.
### ​**5. `product`**
- ​**Purpose**: Stores basic product information.
- ​**Columns**:
    - `product_code` (PK): Unique code for the product.
    - `product_name`: Descriptive name of the product.
### ​**6. `product_model`**
- ​**Purpose**: Stores specific models of a product and their unit price.
- ​**Columns**:
    - `product_code` (PK/FK): Links to the parent product (`product.product_code`).
    - `product_model` (PK): Name of the model (e.g., "Pro Max 256GB").
    - `unit_price`: Price per unit for the model.
### ​**7. `order_detail`**

- ​**Purpose**: Captures detailed information about individual orders.
- ​**Columns**:
    - `order_id` (PK): Auto-incremented unique identifier for the order.
    - `contract_number` (FK): Links the order to its parent contract.
    - `product_code` + `product_model` (Composite FK): Specifies the product model ordered.
    - `quantity`: Number of units ordered.
    - `estimated_delivery_date`: Planned delivery date.
    - `lodgement_date`: Actual delivery date.
    - `salesman_number` (FK): Salesperson responsible for the order.