# split the invoices.csv file into 249MB files without breaking any lines
gsplit -C 249m invoices.csv invoices_
header="INVOICE_ID,PARENT_INVOICE_ID,TRANSACTION_ID,ORGANIZATION_ID,TYPE,STATUS,CURRENCY,PAYMENT_CURRENCY,PAYMENT_METHOD,AMOUNT,PAYMENT_AMOUNT,FX_RATE,FX_RATE_PAYMENT"
for file in invoices_*; do 
    # If the filename is not invoices_aa, add the header line
    if [ "$file" != "invoices_aa" ]; then
        echo -e "$header\n$(cat $file)" > "$file"
    fi
    mv "$file" "${file}.csv"; 
done