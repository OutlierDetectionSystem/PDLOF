package triangulation;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.*;

public class Circumcircle extends JPanel {
    Point2D.Double p0, p1, p2;
    public Circumcircle(Point2D.Double q0, Point2D.Double q1,
			Point2D.Double q2) {
	p0 = q0;  p1 = q1;  p2 = q2;
        setBackground(Color.white);
    } 

    public void paintComponent(Graphics gfx) {
	// usual stuff
        super.paintComponent(gfx);
        Graphics2D g = (Graphics2D) gfx;
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
                           RenderingHints.VALUE_ANTIALIAS_ON);

	// find circumcenter using vector operations
	
	Point2D.Double midDiff = new Point2D.Double((p2.x-p0.x)/2,
						    (p2.y-p0.y)/2);
	Point2D.Double u = new Point2D.Double(p0.y-p1.y, p1.x-p0.x);
	Point2D.Double v = new Point2D.Double(p2.x-p1.x, p2.y-p1.x);
	double t = dot(midDiff, v)/dot(u, v);
	Point2D.Double circumcenter = 
	    new Point2D.Double(t*u.x + (p0.x+p1.x)/2,
			       t*u.y + (p0.y+p1.y)/2);
        double r = p0.distance(circumcenter);

	// set up graphical elements
        Ellipse2D.Double circle =
            new Ellipse2D.Double(circumcenter.x - r, circumcenter.y - r, 
				 2*r, 2*r);
	// set up triangle
	GeneralPath path = new GeneralPath();
	path.moveTo((float) p0.x, (float) p0.y);
	path.lineTo((float) p1.x, (float) p1.y);
	path.lineTo((float) p2.x, (float) p2.y);
	path.closePath();

	// draw circles and triangle
	g.draw(path);
	g.setPaint(new Color(0.5f, 0.5f, 1f));
        g.draw(circle);  

	// add points
        double size = 2;
        Ellipse2D.Double[] points = new Ellipse2D.Double[3];
        points[0] = getDot(p0);   points[1] = getDot(p1);
        points[2] = getDot(p2);

        g.setPaint(new Color(0x33, 0x33, 0xcc));
        for (int i = 0; i < 3; i++) g.fill(points[i]);

        g.setPaint(Color.black);
        for (int i = 0; i < 3;  i++) g.draw(points[i]);

	// add labels
	g.setFont(new Font("sanserif", Font.BOLD, 12));
	g.drawString("A", (float)p0.x + 3, (float) p0.y - 3);
	g.drawString("B", (float)p1.x + 3, (float) p1.y - 3);
	g.drawString("C", (float)p2.x + 3, (float) p2.y - 3);
    }

    public double dot(Point2D.Double q0, Point2D.Double q1) {
	return q0.x*q1.x + q0.y*q1.y;
    }

    public Ellipse2D.Double getDot(Point2D.Double p) {
        double size = 3;
        return new Ellipse2D.Double(p.x - size, p.y - size,
                                    2*size, 2*size);
    }

    public static void main(String[] args) {
	double x0 = Double.parseDouble("10");
	double y0 = Double.parseDouble("10");
	double x1 = Double.parseDouble("30");
	double y1 = Double.parseDouble("40");
	double x2 = Double.parseDouble("40");
	double y2 = Double.parseDouble("0");

        Circumcircle circ = new Circumcircle(new Point2D.Double(x0, y0),
					     new Point2D.Double(x1, y1),
					     new Point2D.Double(x2, y2));
        circ.setPreferredSize(new Dimension(300, 300));
        JFrame frame = new JFrame("Circumcircle");
	frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.getContentPane().add(circ);
        frame.pack();
        frame.setVisible(true);
    }
        
}
    
