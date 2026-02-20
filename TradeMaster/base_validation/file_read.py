from utility.library import *

def contract_read(exch):
    contract = pd.read_csv("%s.csv" % exch)
    # Remove rows where all column values are NaN
    contract = contract.dropna(how='all')

    if 'Strike Price' in contract.columns:
        contract['Strike Price'] = contract['Strike Price'].apply(
            lambda x: int(x) if pd.notnull(x) and isinstance(x, (np.float64, float)) and x.is_integer() else x
        )
        contract['Strike Price'] = contract['Strike Price'].astype(str)
    if 'Token' in contract.columns:
        # Convert valid float values to int, invalid values will be set to NaN
        contract['Token'] = pd.to_numeric(contract['Token'], errors='coerce').astype('Int64')
        contract['Token'] = contract['Token'].astype(str)
    if 'Lot Size' in contract.columns:
        contract['Lot Size'] = contract['Lot Size'].apply(
            lambda x: int(x) if pd.notnull(x) and isinstance(x, (np.float64, float)) else x
        )
        contract['Lot Size'] = contract['Lot Size'].astype(str)

    return contract