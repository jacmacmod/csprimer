#include <stdio.h>
#include <sys/wait.h>
#include <unistd.h>
#ifndef TIOCGWINSZ
#include <sys/ioctl.h>
#endif
#define STB_IMAGE_IMPLEMENTATION
#include "stb_image.h"
#define STB_IMAGE_RESIZE_IMPLEMENTATION
#include "stb_image_resize2.h"

static void sig_winch(int);
static void draw();
char get_ascii_char(int);

int main(void)
{
  // draw();
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
  struct winsize size;

  int fd = STDIN_FILENO;
  if (ioctl(fd, TIOCGWINSZ, (char *)&size) < 0)
    printf("TIOCSGWINSZ error");
  printf("%d rows, %d columns\n", size.ws_row, size.ws_col);
  // printf("\033[H\033[J");
  int w, h, channels;

  unsigned char *img_data = stbi_load("/Users/jack/csprimer/os/proc/img.png",
                                      &w, &h, &channels, 0);
  if (img_data == NULL)
  {
    printf("Error in loading the image\n");
  }
  int sw, sh;
  sw = size.ws_row;
  sh = size.ws_col;
  unsigned char *img_data_scaled = (unsigned char *)malloc(sw * sh * channels);

  stbir_resize_uint8_srgb(img_data, w, h, 0,
                          img_data_scaled, sw, sh, 0, channels == 4 ? STBIR_RGBA : STBIR_RGB);

  if (img_data_scaled == NULL)
  {
    printf("Memory allocation failed\n");
    exit(1);
  }

  // printf("%d x %d --> %d x %d\n", w, h, sw, sh);
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
  stbi_image_free(img_data);
  free(img_data_scaled);
}

char get_ascii_char(int gray_value)
{
  const char *ascii_chars = "@#S%?*+;:,.";
  int index = gray_value * (strlen(ascii_chars) - 1) / 255;
  return ascii_chars[index];
}
