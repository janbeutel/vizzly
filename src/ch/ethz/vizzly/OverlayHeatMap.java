/*
 * Copyright 2012 ETH Zurich, Computer Engineering and Networks Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.ethz.vizzly;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.image.BufferedImage;
import java.awt.image.FilteredImageSource;
import java.awt.image.ImageFilter;
import java.awt.image.ImageProducer;
import java.awt.image.RGBImageFilter;

/**
 * This class implements a first test of generating images that are then put over the map.
 * Will probably be removed very soon.
 * @author Matthias Keller
 *
 */
public class OverlayHeatMap {

    public OverlayHeatMap() {
    }

    public BufferedImage getImage() {
        int height = 500;
        int width = 500;

        BufferedImage bufferedImage = new BufferedImage(width, height, 
                BufferedImage.TYPE_INT_ARGB);

        Graphics2D g2d = bufferedImage.createGraphics();

        // First fill everything with color that is then replaced later
        Color transColor = Color.GRAY;
        g2d.setColor(transColor);
        g2d.fillRect(0, 0, width, height);
        
        // Draw sth in red, should not be transparent later
        g2d.setColor(Color.red);
        g2d.fillRect(0, 0, 100, 100);

        // And check green
        g2d.setColor(Color.green);
        g2d.fillRect(width-100, height-100, 100, 100);
        
        // And check yellow
        g2d.setColor(Color.yellow);
        g2d.fillRect(width-100, 0, 100, 100);
        
        g2d.dispose();

        return _imageToBufferedImage(_transformGrayToTransparency(bufferedImage, transColor));
    }

    private BufferedImage _imageToBufferedImage(Image image) {
        BufferedImage bufferedImage = new BufferedImage(image.getWidth(null), image.getHeight(null), BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2 = bufferedImage.createGraphics();
        g2.drawImage(image, 0, 0, null);
        g2.dispose();
        return bufferedImage;
    }
    
    private Image _transformGrayToTransparency(BufferedImage image, Color c1)
    {
        final int r1 = c1.getRed();
        final int g1 = c1.getGreen();
        final int b1 = c1.getBlue();
        ImageFilter filter = new RGBImageFilter()
        {
            public final int filterRGB(int x, int y, int rgb)
            {
                int r = (rgb & 0xFF0000) >> 16;
                int g = (rgb & 0xFF00) >> 8;
                int b = rgb & 0xFF;
                if (r == r1 && g == g1 && b == b1)
                {
                  // Set fully transparent but keep color
                  return rgb & 0xFFFFFF;
                }
                return rgb;
            }
        };

        ImageProducer ip = new FilteredImageSource(image.getSource(), filter);
        return Toolkit.getDefaultToolkit().createImage(ip);
    }

}
