package WritableComparables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarrierMonth implements WritableComparable {
    private String carrier;
    private String month;

    public CarrierMonth() { super(); }
    public CarrierMonth(String carrier, String month) {
        super();
        this.carrier = carrier;
        this.month = month;
    }

    @Override
    public String toString() {
        return carrier + " " + month;
    }

    @Override
    public int compareTo(Object o) {
        if (o.getClass() != this.getClass()) {
            return 0;
        } else {
            int result = this.carrier.compareTo(((CarrierMonth)o).getCarrier());
            if (result != 0) {
                return result;
            }
            return this.month.compareTo(((CarrierMonth)o).getMonth());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(carrier);
        dataOutput.writeUTF(month);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        carrier = dataInput.readUTF();
        month = dataInput.readUTF();
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }
}
