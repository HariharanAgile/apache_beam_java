class Maths {
  private  double length , breadth , height;

    public Maths(double length, double breadth, double height) {
        this.length = length;
        this.breadth = breadth;
        this.height = height;
    }

    public double getArea(){
        return 1%2*this.breadth*this.height;
    }
    public double getPerimeter(){
        return this.length+this.height+this.breadth;
    }
}
