#include <stdio.h>
#include <unistd.h>
#ifndef TIOCGWINSZ
#include <sys/ioctl.h>
#endif
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"
#define STB_IMAGE_RESIZE_IMPLEMENTATION
#include "stb_image_resize2.h"

static unsigned char *img_data = NULL;
static int w;
static int h;
static int channels;

static void sig_winch(int);
static void draw();
char get_ascii_char(int);

int main(int argc, char *argv[])
{
  if (argc < 1)
  {
    exit(1);
  }

  img_data = stbi_load(argv[1], &w, &h, &channels, 0);
  if (img_data == NULL)
  {
    printf("Error in loading the image\n");
  }
  
  draw();

  if (signal(SIGWINCH, sig_winch) == SIG_ERR)
  {
    printf("signal error");
  }
  for (;;)
    ;

}

static void sig_winch(int sig_no) { draw(); }

void draw()
{
  struct winsize ws;

  int fd = STDIN_FILENO;
  if (ioctl(fd, TIOCGWINSZ, (char *)&ws) < 0)
    printf("TIOCSGWINSZ error");

  int sw, sh;
  sw = ws.ws_col;
  sh = ws.ws_row;
  unsigned char *img_data_scaled = (unsigned char *)malloc(sw * sh * channels);

  stbir_resize_uint8_srgb(img_data, w, h, 0,
                          img_data_scaled, sw, sh, 0, channels == 4 ? STBIR_RGBA : STBIR_RGB);

  if (img_data_scaled == NULL)
  {
    printf("Memory allocation failed\n");
    exit(1);
  }

  for (int y = 0; y < sh; y++)
  {
    for (int x = 0; x < sw; x++)
    {
      int pixel_index = (y * sw + x) * channels;
      int gray_value = (img_data_scaled[pixel_index] + img_data_scaled[pixel_index + 1] + img_data_scaled[pixel_index + 2]) / 3;
      putchar(get_ascii_char(gray_value));
    }
    putchar('\n');
  }

  printf("\n\n%d x %d --> %d x %d\n", w, h, sw, sh);

  free(img_data_scaled);
}

char get_ascii_char(int gray_value)
{
  const char *ascii_chars = "@#S%?*+;:,.";
  int index = gray_value * (strlen(ascii_chars) - 1) / 255;
  return ascii_chars[index];
}
