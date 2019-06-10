package com.randeepbydesign.pubsub.domain;

public class Bottle {
    private int fluidOunces;
    private String label;
    private boolean isEmpty;
    private boolean isPoison;

    public Bottle() {
    }

    public int getFluidOunces() {
        return fluidOunces;
    }

    public void setFluidOunces(int fluidOunces) {
        this.fluidOunces = fluidOunces;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public void setEmpty(boolean empty) {
        isEmpty = empty;
    }

    public boolean isPoison() {
        return isPoison;
    }

    public void setPoison(boolean poison) {
        isPoison = poison;
    }

    @Override
    public String toString() {
        return "Bottle{" +
                "fluidOunces=" + fluidOunces +
                ", label='" + label + '\'' +
                ", isEmpty=" + isEmpty +
                ", isPoison=" + isPoison +
                '}';
    }

}
