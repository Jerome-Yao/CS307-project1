# E-R diagram

- The E-R diagram is drawn on the processon

# Database Design


(create table statements file is in `/create_table.sql`)

## Content Description
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

# Data Import

in src/

| Script name               | Author      | Description                                  |
| ------------------------- | ----------- | -------------------------------------------- |
| /Java/Load.java           | Yao Shengqi | The main function. Run this with parameters  |
| /Java/LowLoad.java        | Yao Shengqi | The class that imports the data in serial    |
| /Java/ConcurrentLoad.java | Yao Shengqi | The class that imports the data concurrently |
| /Java/PrepareTool.java    | Yao Shengqi | The class that provides preparing method     |

### How to use

#### Java
1. Import all the .jar file in `/dependencies`. Notice the postgresql dependency is not included.
2. Use `create_table.sql` to create table.
3. Modify the `resources/dbUser.properties`. Edit the 'database', 'user', 'pwd', 'port'
4. Move the original (or modified) .csv file under path `/resources`. Rename the file `output25S.csv`
5. Open `/src/Java/Load.java`, run with parameters:
	1. `0` concurrent mode
	2. `1` serial mode


# Advanced

### Optimization

#### Java
- In concurrent mode, the script first parses the data and stores it into a list. It then imports the data in three concurrent batches based on parent-child table relationships. Each batch is mutually independent, allowing concurrent imports. Lower-level data depends on the completion of higher-level data. The third batch exclusively imports the ​order_details​ table by splitting the list into multiple sub-batches for multi-threaded import.
#### Comparision  


### Multiple System Supported
#### MacOS
