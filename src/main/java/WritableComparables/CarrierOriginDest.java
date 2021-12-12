package WritableComparables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarrierOriginDest implements WritableComparable {
    private String carrier;
    private String origin;
    private String destination;

    public CarrierOriginDest() { super(); }
    public CarrierOriginDest(String carrier, String origin, String destination)
    {
        super();
        this.carrier = carrier;
        this.origin = origin;
        this.destination = destination;
    }

    @Override
    public String toString() {
        return carrier + " " + origin + " " + destination;
    }

    @Override
    public int compareTo(Object o) {
        if (o.getClass() != this.getClass()) {
            return 0;
        } else {
            int result = this.carrier.compareTo(((CarrierOriginDest)o).getCarrier());
            if (result != 0) {
                return result;
            }
            result = this.origin.compareTo(((CarrierOriginDest)o).getOrigin());
            if (result != 0) {
                return result;
            }
            result = this.destination.compareTo(((CarrierOriginDest)o).getDestination());
            if (result != 0) {
                return result;
            }
            return  0;
        }

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(carrier);
        dataOutput.writeUTF(origin);
        dataOutput.writeUTF(destination);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        carrier = dataInput.readUTF();
        origin = dataInput.readUTF();
        destination = dataInput.readUTF();
    }

    public String getCarrier() {
        return carrier;
    }

    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }
}
